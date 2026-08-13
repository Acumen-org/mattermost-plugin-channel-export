// Copyright (c) 2020-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/mattermost/mattermost-plugin-channel-export/server/pluginapi"
	"github.com/mattermost/mattermost-plugin-channel-export/server/util"
)

const (
	KeyClusterMutex = "mutex_exporter"
)

// Handler encapsulates the context necessary for the channel export API.
type Handler struct {
	client            *pluginapi.Wrapper
	makePostsIterator func(*model.Channel, bool, ExportFilter) PostIterator
	clusterMutex      pluginapi.ClusterMutex
	plugin            *Plugin
}

// registerAPI registers the API against the given router.
func registerAPI(plugin *Plugin, makePostsIterator func(*model.Channel, bool, ExportFilter) PostIterator) error {
	clusterMutex, err := plugin.client.Cluster.NewMutex(KeyClusterMutex)
	if err != nil {
		return fmt.Errorf("cannot create cluster mutex: %w", err)
	}

	handler := &Handler{
		client:            plugin.client,
		makePostsIterator: makePostsIterator,
		clusterMutex:      clusterMutex,
		plugin:            plugin,
	}

	api := handler.plugin.router.PathPrefix("/api/v1").Subrouter()
	api.Use(mattermostAuthorizationRequired)
	api.HandleFunc("/export", handler.Export)
	api.HandleFunc("/export/dialog", handler.ExportDialog).Methods(http.MethodPost)
	return nil
}

// APIError is a type of error returned by the API.
type APIError struct {
	StatusText string
	Message    string
	StatusCode int
}

func (e *APIError) Error() string {
	return e.Message
}

func handleError(w http.ResponseWriter, statusCode int, message string, a ...any) {
	message = fmt.Sprintf(message, a...)
	logrus.Warnf("%s (%d): %s", http.StatusText(statusCode), statusCode, message)

	w.WriteHeader(statusCode)
	apiErr := APIError{
		StatusCode: statusCode,
		StatusText: http.StatusText(statusCode),
		Message:    message,
	}
	b, _ := json.Marshal(apiErr) //nolint:errcheck // json.Marshal on a fixed struct never fails
	_, err := w.Write(b)
	if err != nil {
		logrus.WithError(err).Warnf("failed to handle error")
	}
}

// mattermostAuthorizationRequired requires a Mattermost user to have authenticated.
func mattermostAuthorizationRequired(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("Mattermost-User-ID")
		if userID != "" {
			next.ServeHTTP(w, r)
			return
		}

		http.Error(w, "Not authorized", http.StatusUnauthorized)
	})
}

func (h *Handler) hasPermissionToChannel(userID, channelID string) (*model.Channel, bool) {
	channel, err := h.client.Channel.Get(channelID)
	if appErr, ok := err.(*model.AppError); ok && appErr.StatusCode == http.StatusNotFound {
		return nil, false
	} else if err != nil {
		logrus.Warnf("failed to query channel '%s'", channelID)
		return nil, false
	}

	if h.client.User.HasPermissionToChannel(userID, channelID, model.PermissionReadChannel) {
		return channel, true
	}

	return nil, false
}

// Export handles /api/v1/export, exporting the requested channel.
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
	// only allow one export at a time
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	if err := h.clusterMutex.LockWithContext(ctx); err != nil {
		handleError(w, http.StatusServiceUnavailable, "a channel export is already running.")
		return
	}
	defer func() {
		h.clusterMutex.Unlock()
	}()

	license := h.client.System.GetLicense()
	if !isLicensed(license, h.client) {
		handleError(w, http.StatusBadRequest, "the channel export plugin requires a valid Enterprise license.")
		return
	}

	channelID := r.URL.Query().Get("channel_id")
	if channelID == "" {
		handleError(w, http.StatusBadRequest, "missing channel_id parameter")
		return
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		handleError(w, http.StatusBadRequest, "missing format parameter")
		return
	}
	if format != "csv" {
		handleError(w, http.StatusBadRequest, "unsupported format parameter '%s'", format)
		return
	}

	userID := r.Header.Get("Mattermost-User-ID")
	channel, ok := h.hasPermissionToChannel(userID, channelID)
	if !ok {
		handleError(w, http.StatusNotFound, "channel '%s' not found or user does not have permission", channelID)
		return
	}

	areArchivedChannelsVisible := h.client.Configuration.GetConfig().TeamSettings.ExperimentalViewArchivedChannels != nil && *h.client.Configuration.GetConfig().TeamSettings.ExperimentalViewArchivedChannels

	if channel.DeleteAt > 0 && !areArchivedChannelsVisible {
		handleError(w, http.StatusNotFound, "channel '%s' is archived and not visible anymore", channelID)
		return
	}

	if !h.plugin.hasPermissionToExportChannel(userID, channelID) {
		handleError(w, http.StatusForbidden, "user does not have permission to export channels")
		return
	}

	sinceStr := r.URL.Query().Get("since")
	untilStr := r.URL.Query().Get("until")

	since, err := parseDateParam(sinceStr)
	if err != nil {
		handleError(w, http.StatusBadRequest, "invalid since parameter: use YYYY-MM-DD format")
		return
	}
	until, err := parseDateParam(untilStr)
	if err != nil {
		handleError(w, http.StatusBadRequest, "invalid until parameter: use YYYY-MM-DD format")
		return
	}
	if !until.IsZero() {
		until = until.Add(24*time.Hour - time.Nanosecond)
	}
	if !since.IsZero() && !until.IsZero() && since.After(until) {
		handleError(w, http.StatusBadRequest, "since must be before until")
		return
	}
	filter := ExportFilter{Since: since, Until: until}

	postIterator := h.makePostsIterator(channel, showEmailAddress(h.client, userID), filter)

	exporter := CSV{}
	fileName := exporter.FileName(channel.Name, filter)

	w.Header().Set("Content-Disposition", "attachment; filename="+fileName)
	w.Header().Set("Content-Type", exporter.ContentType())
	if err := exporter.Export(postIterator, w); err != nil {
		handleError(w, http.StatusInternalServerError, "failed to create the exported data")
	}
}

// parseDateParam parses an optional YYYY-MM-DD date string in UTC.
// An empty string returns a zero time.Time with no error.
func parseDateParam(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.ParseInLocation("2006-01-02", value, time.UTC)
}

// ExportDialog handles POST /api/v1/export/dialog — the interactive dialog submission.
func (h *Handler) ExportDialog(w http.ResponseWriter, r *http.Request) {
	var req model.SubmitDialogRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		handleError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	dialogError := func(msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := model.SubmitDialogResponse{Error: msg}
		b, _ := json.Marshal(resp) //nolint:errcheck // json.Marshal on a fixed struct never fails
		_, _ = w.Write(b)
	}

	fromStr, _ := req.Submission["from"].(string)
	toStr, _ := req.Submission["to"].(string)

	since, err := parseDateParam(fromStr)
	if err != nil {
		dialogError(fmt.Sprintf("Invalid from date %q, use YYYY-MM-DD format.", fromStr))
		return
	}
	until, err := parseDateParam(toStr)
	if err != nil {
		dialogError(fmt.Sprintf("Invalid to date %q, use YYYY-MM-DD format.", toStr))
		return
	}
	if !until.IsZero() {
		until = until.Add(24*time.Hour - time.Nanosecond)
	}
	if !since.IsZero() && !until.IsZero() && since.After(until) {
		dialogError("From date must be before to date.")
		return
	}

	filter := ExportFilter{Since: since, Until: until}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	if err := h.clusterMutex.LockWithContext(ctx); err != nil {
		dialogError("An export is already running.")
		return
	}

	channelID := req.ChannelId
	userID := req.UserId

	channel, ok := h.hasPermissionToChannel(userID, channelID)
	if !ok {
		h.clusterMutex.Unlock()
		dialogError("You do not have permission to export this channel.")
		return
	}

	if !h.plugin.hasPermissionToExportChannel(userID, channelID) {
		h.clusterMutex.Unlock()
		dialogError("You do not have permission to export channels.")
		return
	}

	go func() {
		defer h.clusterMutex.Unlock()

		exporter := CSV{}
		fileName := exporter.FileName(channel.Name, filter)
		postIter := h.makePostsIterator(channel, showEmailAddress(h.client, userID), filter)

		channelDM, err := h.client.Channel.GetDirect(userID, h.plugin.botID)
		if err != nil {
			h.client.Log.Error("dialog export: unable to create DM channel", "Error", err)
			return
		}

		pr, pw := io.Pipe()
		limitedWriter := util.NewLimitPipeWriter(pw, h.plugin.getMaxFileSize())

		go func() {
			if exportErr := exporter.Export(postIter, limitedWriter); exportErr != nil {
				_ = limitedWriter.CloseWithError(exportErr)
				return
			}
			_ = limitedWriter.Close()
		}()

		file, err := h.plugin.uploadFileTo(fileName, pr, channelDM.Id)
		if err != nil {
			_ = h.client.Post.CreatePost(&model.Post{
				UserId:    h.plugin.botID,
				ChannelId: channelDM.Id,
				Message:   fmt.Sprintf("Export failed: %s", err.Error()),
			})
			return
		}

		successMsg := fmt.Sprintf("Channel ~%s exported:", channel.Name)
		_ = h.client.Post.CreatePost(&model.Post{
			UserId:    h.plugin.botID,
			ChannelId: channelDM.Id,
			Message:   successMsg,
			FileIds:   []string{file.Id},
		})
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(model.SubmitDialogResponse{}) //nolint:errcheck // json.Marshal on a fixed struct never fails
	_, _ = w.Write(b)
}
