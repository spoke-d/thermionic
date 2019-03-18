package db_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestIsOffline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tc := range []struct {
		name     string
		input    time.Time
		expected bool
	}{
		{
			name:     "offline",
			input:    time.Now(),
			expected: false,
		},
		{
			name:     "online",
			input:    time.Now().Add(-time.Minute),
			expected: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClock := mocks.NewMockClock(ctrl)
			mockClock.EXPECT().Now().Return(time.Now())

			nodeInfo := db.NodeInfo{
				Heartbeat: tc.input,
			}
			ok := nodeInfo.IsOffline(mockClock, time.Second)
			if expected, actual := tc.expected, ok; expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
		})
	}
}

func TestVersion(t *testing.T) {
	nodeInfo := db.NodeInfo{
		Schema:        1,
		APIExtensions: 10,
	}
	if expected, actual := [2]int{1, 10}, nodeInfo.Version(); !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
