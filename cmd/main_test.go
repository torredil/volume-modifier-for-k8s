package main

import (
	"context"
	"testing"
	"time"

	csi "github.com/awslabs/volume-modifier-for-k8s/pkg/client"
	"github.com/awslabs/volume-modifier-for-k8s/pkg/controller"

	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/mock/gomock"
)

// workerCount is the default value of the *workers flag used by leaseHandler.
const workerCount = 10

func TestLeaseHandler_PodIsLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)

	signalChannel := make(chan struct{}, 1)
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Do(
		func(_ int, _ context.Context) {
			t.Log("Run was called")
			signalChannel <- struct{}{}
		},
	).Times(1)

	lease := newLease("external-resizer-ebs-csi-aws-com", "test-pod")
	runLeaseHandlerAndSendLease(t, "test-pod", mockModifyController, lease)

	select {
	case <-signalChannel:
		t.Log("Signal received, test passed")
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for Run to be called")
	}
}

func TestLeaseHandler_PodIsNotLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Times(0)

	lease := newLease("external-resizer-ebs-csi-aws-com", "other-pod")
	runLeaseHandlerAndSendLease(t, "test-pod", mockModifyController, lease)
}

func TestLeaseHandler_PodRegainsLeadership(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)

	firstSignalChannel := make(chan struct{}, 1)
	secondSignalChannel := make(chan struct{}, 1)

	// Expect Run to be called once initially, signal via first channel
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Do(
		func(_ int, _ context.Context) {
			t.Log("First Run was called")
			firstSignalChannel <- struct{}{}
		},
	).Times(1)

	// Expect Run to be called again after leadership loss, signal via second channel
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Do(
		func(_ int, _ context.Context) {
			t.Log("Second Run was called")
			secondSignalChannel <- struct{}{}
		},
	).Times(1)

	leaseChannel := make(chan *v1.Lease, 3)
	defer close(leaseChannel)

	go leaseHandler("test-pod", func() controller.ModifyController { return mockModifyController }, leaseChannel)

	// Become the leader
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "test-pod")
	<-firstSignalChannel
	// Lose the leadership
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "other-pod")
	// Regain the leadership
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "test-pod")

	select {
	case <-secondSignalChannel:
		t.Log("Test passed, second Run was called")
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for Run to be called")
	}
}

func TestLeaseHandler_RunCalledOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)

	signalChannel := make(chan struct{}, 1)

	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Do(
		func(_ int, _ context.Context) {
			t.Log("Run was called")
			signalChannel <- struct{}{}
		},
	).Times(1)

	leaseChannel := make(chan *v1.Lease, 3)
	defer close(leaseChannel)

	go leaseHandler("test-pod", func() controller.ModifyController { return mockModifyController }, leaseChannel)

	for i := 0; i < 10; i++ {
		leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "test-pod")
	}

	select {
	case <-signalChannel:
		t.Log("Signal received, test passed")
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for Run to be called")
	}
}

func TestGetDriverName(t *testing.T) {
	client := csi.NewFakeClient("ebs.csi.aws.com", true, false)
	name, err := getDriverName(client, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "ebs.csi.aws.com" {
		t.Fatalf("expected driver name %q, got %q", "ebs.csi.aws.com", name)
	}
}

func TestLeaseHandler_ChannelClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)

	signalChannel := make(chan struct{}, 1)
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Do(
		func(_ int, ctx context.Context) {
			t.Log("Run was called")
			signalChannel <- struct{}{}
			<-ctx.Done()
		},
	).Times(1)

	leaseChannel := make(chan *v1.Lease, 2)

	done := make(chan struct{})
	go func() {
		leaseHandler("test-pod", func() controller.ModifyController { return mockModifyController }, leaseChannel)
		close(done)
	}()

	// Become leader, then close channel
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "test-pod")
	<-signalChannel
	close(leaseChannel)

	select {
	case <-done:
		t.Log("leaseHandler returned after channel close")
	case <-time.After(2 * time.Second):
		t.Fatal("leaseHandler did not return after channel close")
	}
}

func newLease(name, holderIdentity string) *v1.Lease {
	return &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.LeaseSpec{
			HolderIdentity: &holderIdentity,
		},
	}
}

func runLeaseHandlerAndSendLease(t *testing.T, podName string, mockModifyController *controller.MockModifyController, lease *v1.Lease) {
	leaseChannel := make(chan *v1.Lease, 1)
	go leaseHandler(podName, func() controller.ModifyController { return mockModifyController }, leaseChannel)
	leaseChannel <- lease
	close(leaseChannel)
}

func TestLeaseHandler_NonLeaderToNonLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)
	// Run should never be called since we're never the leader
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Times(0)

	leaseChannel := make(chan *v1.Lease, 3)

	done := make(chan struct{})
	go func() {
		leaseHandler("test-pod", func() controller.ModifyController { return mockModifyController }, leaseChannel)
		close(done)
	}()

	// Multiple lease updates where someone else is always the leader
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "other-pod-1")
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "other-pod-2")
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "other-pod-3")
	close(leaseChannel)

	select {
	case <-done:
		t.Log("leaseHandler returned cleanly")
	case <-time.After(2 * time.Second):
		t.Fatal("leaseHandler did not return after channel close")
	}
}

func TestLeaseHandler_ChannelClosedWhileNotLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockModifyController := controller.NewMockModifyController(ctrl)
	mockModifyController.EXPECT().Run(gomock.Eq(workerCount), gomock.Not(gomock.Nil())).Times(0)

	leaseChannel := make(chan *v1.Lease, 1)

	done := make(chan struct{})
	go func() {
		leaseHandler("test-pod", func() controller.ModifyController { return mockModifyController }, leaseChannel)
		close(done)
	}()

	// Never become leader, just close
	leaseChannel <- newLease("external-resizer-ebs-csi-aws-com", "other-pod")
	close(leaseChannel)

	select {
	case <-done:
		t.Log("leaseHandler returned after channel close while not leader")
	case <-time.After(2 * time.Second):
		t.Fatal("leaseHandler did not return")
	}
}
