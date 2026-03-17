package util

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPVCKey(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		pvcName   string
		expected  string
	}{
		{"standard", "default", "my-pvc", "default/my-pvc"},
		{"custom namespace", "kube-system", "data-pvc", "kube-system/data-pvc"},
		{"empty namespace", "", "my-pvc", "/my-pvc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.pvcName,
					Namespace: tc.namespace,
				},
			}
			got := PVCKey(pvc)
			if got != tc.expected {
				t.Errorf("PVCKey() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestGetPatchData(t *testing.T) {
	t.Run("successful patch generation", func(t *testing.T) {
		old := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pv",
				Annotations: map[string]string{"key": "old"},
			},
		}
		new := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pv",
				Annotations: map[string]string{"key": "new"},
			},
		}
		patch, err := GetPatchData(old, new)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(patch) == 0 {
			t.Fatal("expected non-empty patch data")
		}
	})

	t.Run("no diff produces minimal patch", func(t *testing.T) {
		obj := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		}
		patch, err := GetPatchData(obj, obj)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if patch == nil {
			t.Fatal("expected non-nil patch data")
		}
	})
}

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"dots replaced", "ebs.csi.aws.com", "ebs-csi-aws-com"},
		{"already clean", "my-driver", "my-driver"},
		{"trailing special char", "driver.", "driver-X"},
		{"underscores replaced", "my_driver_name", "my-driver-name"},
		{"mixed special chars", "a.b_c!d", "a-b-c-d"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeName(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeName(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestGetPatchData_MarshalError(t *testing.T) {
	// Passing an unmarshalable type (channel) should trigger a marshal error
	t.Run("old object marshal error", func(t *testing.T) {
		badObj := make(chan int)
		_, err := GetPatchData(badObj, &v1.PersistentVolume{})
		if err == nil {
			t.Fatal("expected error for unmarshalable old object, got nil")
		}
	})

	t.Run("new object marshal error", func(t *testing.T) {
		_, err := GetPatchData(&v1.PersistentVolume{}, make(chan int))
		if err == nil {
			t.Fatal("expected error for unmarshalable new object, got nil")
		}
	})
}
