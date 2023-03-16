// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	perrors "github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/backup/util"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/pdapi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

var (
	refreshCheckpointTsPeriod = time.Minute * 1
	streamKeyPrefix           = "/tidb/br-stream"
	taskCheckpointPath        = "/checkpoint"
	taskInfoPath              = "/info"
	taskPausePath             = "/pause"
	taskLastErrorPath         = "/last-error"
)

// BackupTracker implements the logic for tracking log backup progress
type BackupTracker interface {
	StartTrackLogBackupProgress(backup *v1alpha1.Backup) error
	DoRefreshLogBackupStatus(backup *v1alpha1.Backup)
}

// the main processes of log backup track:
// a. tracker init will try to find all log backup and add them to the map which key is namespack and cluster.
// b. log backup start will add it to the map
// c. if add log backup to the map successfully, it will start a go routine which has a loop to track log backup's checkpoint ts and will stop when log backup complete.
// d. by the way, add or delete the map has a mutex.
type backupTracker struct {
	deps          *controller.Dependencies
	statusUpdater controller.BackupConditionUpdaterInterface
	operateLock   sync.Mutex
	logBackups    map[string]*trackDepends
}

// trackDepends is the tracker depends, such as tidb cluster info.
type trackDepends struct {
	tc *v1alpha1.TidbCluster
}

// NewBackupTracker returns a BackupTracker
func NewBackupTracker(deps *controller.Dependencies, statusUpdater controller.BackupConditionUpdaterInterface) BackupTracker {
	tracker := &backupTracker{
		deps:          deps,
		statusUpdater: statusUpdater,
		logBackups:    make(map[string]*trackDepends),
	}
	go tracker.initTrackLogBackupsProgress()
	return tracker
}

// initTrackLogBackupsProgress lists all log backups and track their progress.
func (bt *backupTracker) initTrackLogBackupsProgress() {
	var (
		backups *v1alpha1.BackupList
		err     error
	)
	err = retry.OnError(retry.DefaultRetry, func(e error) bool { return e != nil }, func() error {
		backups, err = bt.deps.Clientset.PingcapV1alpha1().Backups("").List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			klog.Warningf("list backups error %v, will retry", err)
			return err
		}
		return nil
	})
	if err != nil {
		klog.Errorf("list backups error %v after retry, skip track all log backups progress when init, will track when log backup start", err)
		return
	}

	klog.Infof("list backups success, size %d", len(backups.Items))
	for i := range backups.Items {
		backup := backups.Items[i]
		if backup.Spec.Mode == v1alpha1.BackupModeLog {
			err = bt.StartTrackLogBackupProgress(&backup)
			if err != nil {
				klog.Warningf("start track log backup %s/%s error %v, will skip and track when log backup start", backup.Namespace, backup.Name, err)
			}
		}
	}
}

// StartTrackLogBackupProgress starts to track log backup progress.
func (bt *backupTracker) StartTrackLogBackupProgress(backup *v1alpha1.Backup) error {
	if backup.Spec.Mode != v1alpha1.BackupModeLog {
		return nil
	}
	ns := backup.Namespace
	name := backup.Name

	bt.operateLock.Lock()
	defer bt.operateLock.Unlock()

	logkey := genLogBackupKey(ns, name)
	if _, exist := bt.logBackups[logkey]; exist {
		klog.Infof("log backup %s/%s has exist in tracker %s", ns, name, logkey)
		return nil
	}
	klog.Infof("add log backup %s/%s to tracker", ns, name)
	tc, err := bt.getLogBackupTC(backup)
	if err != nil {
		return err
	}
	bt.logBackups[logkey] = &trackDepends{tc: tc}
	// go bt.refreshLogBackupCheckpointTs(ns, name)
	go bt.refreshLogBackupStatusAccordingToTikvTask(ns, name)
	return nil
}

// removeLogBackup removes log backup from tracker.
func (bt *backupTracker) removeLogBackup(ns, name string) {
	bt.operateLock.Lock()
	defer bt.operateLock.Unlock()
	delete(bt.logBackups, genLogBackupKey(ns, name))
}

// getLogBackupTC gets log backup's tidb cluster info.
func (bt *backupTracker) getLogBackupTC(backup *v1alpha1.Backup) (*v1alpha1.TidbCluster, error) {
	var (
		ns               = backup.Namespace
		name             = backup.Name
		clusterNamespace = backup.Spec.BR.ClusterNamespace
		tc               *v1alpha1.TidbCluster
		err              error
	)
	if backup.Spec.BR.ClusterNamespace == "" {
		clusterNamespace = ns
	}

	err = retry.OnError(retry.DefaultRetry, func(e error) bool { return e != nil }, func() error {
		tc, err = bt.deps.Clientset.PingcapV1alpha1().TidbClusters(clusterNamespace).Get(context.TODO(), backup.Spec.BR.Cluster, metav1.GetOptions{})
		if err != nil {
			klog.Warningf("get log backup %s/%s tidbcluster %s/%s failed and will retry, err is %v", ns, name, clusterNamespace, backup.Spec.BR.Cluster, err)
			return err
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("get log backup %s/%s tidbcluster %s/%s failed, err is %v", ns, name, clusterNamespace, backup.Spec.BR.Cluster, err)
	}
	return tc, nil
}

// // refreshLogBackupCheckpointTs updates log backup progress periodically.
// func (bt *backupTracker) refreshLogBackupCheckpointTs(ns, name string) {
// 	ticker := time.NewTicker(refreshCheckpointTsPeriod)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		logkey := genLogBackupKey(ns, name)
// 		if _, exist := bt.logBackups[logkey]; !exist {
// 			return
// 		}
// 		backup, err := bt.deps.BackupLister.Backups(ns).Get(name)
// 		if errors.IsNotFound(err) {
// 			klog.Infof("log backup %s/%s has been deleted, will remove %s from tracker", ns, name, logkey)
// 			bt.removeLogBackup(ns, name)
// 			return
// 		}
// 		if err != nil {
// 			klog.Infof("get log backup %s/%s error %v, will skip to the next time", ns, name, err)
// 			continue
// 		}
// 		if backup.DeletionTimestamp != nil || backup.Status.Phase == v1alpha1.BackupComplete {
// 			klog.Infof("log backup %s/%s is being deleting or complete, will remove %s from tracker", ns, name, logkey)
// 			bt.removeLogBackup(ns, name)
// 			return
// 		}
// 		if backup.Status.Phase != v1alpha1.BackupRunning {
// 			klog.Infof("log backup %s/%s is not running, will skip to the next time refresh", ns, name)
// 			continue
// 		}
// 		bt.doRefreshLogBackupCheckpointTs(backup, bt.logBackups[logkey])
// 	}
// }

// // doRefreshLogBackupCheckpointTs gets log backup checkpoint ts from pd and updates log backup cr.
// func (bt *backupTracker) doRefreshLogBackupCheckpointTs(backup *v1alpha1.Backup, dep *trackDepends) {
// 	ns := backup.Namespace
// 	name := backup.Name
// 	etcdCli, err := bt.deps.PDControl.GetPDEtcdClient(pdapi.Namespace(dep.tc.Namespace), dep.tc.Name, dep.tc.IsTLSClusterEnabled())
// 	if err != nil {
// 		klog.Errorf("get log backup %s/%s pd cli error %v", ns, name, err)
// 		return
// 	}
// 	defer etcdCli.Close()
// 	key := path.Join(streamKeyPrefix, taskCheckpointPath, name)
// 	klog.Infof("log backup %s/%s checkpointTS key %s", ns, name, key)

// 	kvs, err := etcdCli.Get(key, true)
// 	if err != nil {
// 		klog.Errorf("get log backup %s/%s checkpointTS error %v", ns, name, err)
// 		return
// 	}
// 	if len(kvs) < 1 {
// 		klog.Errorf("log backup %s/%s checkpointTS not found", ns, name)
// 		return
// 	}
// 	ckTS := strconv.FormatUint(binary.BigEndian.Uint64(kvs[0].Value), 10)

// 	klog.Infof("update log backup %s/%s checkpointTS %s", ns, name, ckTS)
// 	updateStatus := &controller.BackupUpdateStatus{
// 		LogCheckpointTs: &ckTS,
// 	}
// 	err = bt.statusUpdater.Update(backup, nil, updateStatus)
// 	if err != nil {
// 		klog.Errorf("update log backup %s/%s checkpointTS %s failed %v", ns, name, ckTS, err)
// 		return
// 	}
// }

func genLogBackupKey(ns, name string) string {
	return fmt.Sprintf("%s.%s", ns, name)
}

// refreshLogBackupStatusAccordingToTikvTask updates log backup progress periodically.
func (bt *backupTracker) refreshLogBackupStatusAccordingToTikvTask(ns, name string) {
	ticker := time.NewTicker(refreshCheckpointTsPeriod)
	defer ticker.Stop()

	for range ticker.C {
		logkey := genLogBackupKey(ns, name)
		if _, exist := bt.logBackups[logkey]; !exist {
			return
		}
		backup, err := bt.deps.BackupLister.Backups(ns).Get(name)
		if errors.IsNotFound(err) {
			klog.Infof("log backup %s/%s has been deleted, will remove %s from tracker", ns, name, logkey)
			bt.removeLogBackup(ns, name)
			return
		}
		if err != nil {
			klog.Infof("get log backup %s/%s error %v, will skip to the next time", ns, name, err)
			continue
		}
		// if backup.DeletionTimestamp != nil || backup.Status.Phase == v1alpha1.BackupComplete {
		// 	klog.Infof("log backup %s/%s is being deleting or complete, will remove %s from tracker", ns, name, logkey)
		// 	bt.removeLogBackup(ns, name)
		// 	return
		// }
		// if backup.Status.Phase != v1alpha1.BackupRunning {
		// 	klog.Infof("log backup %s/%s is not running, will skip to the next time refresh", ns, name)
		// 	continue
		// }
		bt.DoRefreshLogBackupStatus(backup)
	}
}

// doRefreshLogBackupCheckpointTs gets log backup checkpoint ts from pd and updates log backup cr.
func (bt *backupTracker) DoRefreshLogBackupStatus(backup *v1alpha1.Backup) {
	var (
		ns       = backup.Namespace
		name     = backup.Name
		etcdCli  pdapi.PDEtcdClient
		tikvTask *tikvTaskStatus
		err      error
	)

	tc, err := bt.getLogBackupTC(backup)
	if err != nil {
		klog.Errorf("fail to get log backup %s/%s tidb cluster %v", ns, name, err)
		return
	}

	etcdCli, err = bt.deps.PDControl.GetPDEtcdClient(pdapi.Namespace(tc.Namespace), tc.Name, tc.IsTLSClusterEnabled())
	if err != nil {
		klog.Errorf("get log backup %s/%s pd cli error %v", ns, name, err)
		return
	}
	defer etcdCli.Close()

	tikvTask, err = getTikvTaskStatusFromPD(backup, etcdCli)
	if err != nil {
		klog.Errorf("fail to get tikv task for log backup %s/%s, error is %v", ns, name, err)
		return
	}

	newCondition, newStatus := findOutDiffStatusBetweenCRAndTikvTask(backup, tikvTask)
	klog.Infof("will update log backup %s/%s status, newCondition %v, newStatus %v", ns, name, newCondition, newStatus)

	if newCondition == nil && newStatus == nil {
		return
	}

	err = bt.statusUpdater.Update(backup, newCondition, newStatus)
	if err != nil {
		klog.Errorf("update log backup %s/%s status failed, newCondition %v, newStatus %v, error %v", ns, name, newCondition, newStatus, err)
		return
	}
}

type tikvTaskStatus struct {
	info         *backuppb.StreamBackupTaskInfo
	isPause      bool
	lastError    map[uint64]backuppb.StreamBackupError
	checkPointTS *string
}

func getTikvTaskStatusFromPD(backup *v1alpha1.Backup, etcdCli pdapi.PDEtcdClient) (*tikvTaskStatus, error) {
	ns := backup.Namespace
	name := backup.Name
	tikvTaskInfo, err := getTikvTaskInfo(backup, etcdCli)
	if err != nil {
		klog.Errorf("fail to get log backup %s/%s tikv task info, error %v", ns, name, err)
		return nil, err
	}

	isPause, err := isTikvTaskPause(backup, etcdCli)
	if err != nil {
		klog.Errorf("fail to get whether log backup %s/%s tikv task is pause, error %v", ns, name, err)
		return nil, err
	}

	lastError, err := getTikvTaskError(backup, etcdCli)
	if err != nil {
		klog.Errorf("fail to get log backup %s/%s tikv task error message, error %v", ns, name, err)
		return nil, err
	}

	checkPointTS, err := getTikvTaskCheckpointTS(backup, etcdCli)
	if err != nil {
		klog.Errorf("fail to get log backup %s/%s tikv task checkpoint ts, error %v", ns, name, err)
		return nil, err
	}

	return &tikvTaskStatus{
		info:         tikvTaskInfo,
		isPause:      isPause,
		lastError:    lastError,
		checkPointTS: checkPointTS,
	}, nil
}

func findOutDiffStatusBetweenCRAndTikvTask(backup *v1alpha1.Backup, tikvTask *tikvTaskStatus) (*v1alpha1.BackupCondition, *controller.BackupUpdateStatus) {
	if v1alpha1.ParseLogBackupSubcommand(backup) == "" || tikvTask == nil {
		return nil, nil
	}

	var (
		// tikv task status
		isTikvTaskExist = tikvTask.info != nil
		// isTikvTaskError = tikvTask.isPause && len(tikvTask.lastError) > 0
		// cr status
		subCommand      = v1alpha1.ParseLogBackupSubcommand(backup)
		isCRBeforeStart = subCommand == v1alpha1.LogStartCommand && backup.Status.Phase == ""
		isCRStarting    = subCommand == v1alpha1.LogStartCommand && !v1alpha1.IsBackupComplete(backup) && !v1alpha1.IsBackupFailed(backup)
		isCRStopping    = subCommand == v1alpha1.LogStopCommand && !v1alpha1.IsBackupComplete(backup) && !v1alpha1.IsBackupFailed(backup)
		isCRDeleting    = backup.DeletionTimestamp != nil
		isCRFailed      = backup.Status.Phase == v1alpha1.BackupFailed
		isCRRunning     = !isCRBeforeStart && !isCRStarting || !isCRStopping && !isCRDeleting && !isCRFailed
	)

	if isCRBeforeStart {
		return createDiffStatusTheSameAsTikvTask(backup, tikvTask)
	}

	if isCRRunning {
		if !isTikvTaskExist {
			// todo restart
			return &v1alpha1.BackupCondition{
				Type:    v1alpha1.BackupFailed,
				Status:  corev1.ConditionTrue,
				Reason:  "TikvTaskMissing",
				Message: "Tikv Task is missing",
			}, nil
		}
		return createDiffStatusTheSameAsTikvTask(backup, tikvTask)
	}

	if isCRDeleting {
		if isTikvTaskExist {
			// todo stop
			return nil, nil
		}
	}

	return nil, nil
}

func createDiffStatusTheSameAsTikvTask(backup *v1alpha1.Backup, tikvTask *tikvTaskStatus) (*v1alpha1.BackupCondition, *controller.BackupUpdateStatus) {
	if tikvTask == nil {
		return nil, nil
	}

	var (
		// tikv task status
		isTikvTaskExist = tikvTask.info != nil
		isTikvTaskError = tikvTask.isPause && len(tikvTask.lastError) > 0
	)

	if !isTikvTaskExist {
		return nil, nil
	}

	if isTikvTaskError {
		// TODO resume
		return &v1alpha1.BackupCondition{
			Type:    v1alpha1.BackupFailed,
			Status:  corev1.ConditionTrue,
			Reason:  "DetectedTikvTaskFailed",
			Message: fmt.Sprintf("Task Error %v", tikvTask.lastError),
		}, nil
	}
	commitTS := strconv.FormatUint(tikvTask.info.GetStartTs(), 10)
	backupPath, err := util.GetStoragePath(backup.Spec.StorageProvider)
	if err != nil {
		klog.Errorf("fail to get storage path from log backup %s/%s spec, error %v", backup.Namespace, backup.Name, err)
		return nil, nil
	}

	return &v1alpha1.BackupCondition{
			Type:   v1alpha1.BackupRunning,
			Status: corev1.ConditionTrue,
		}, &controller.BackupUpdateStatus{
			BackupPath:      &backupPath,
			CommitTs:        &commitTS,
			LogCheckpointTs: tikvTask.checkPointTS,
		}
}

func getTikvTaskInfo(backup *v1alpha1.Backup, etcdCli pdapi.PDEtcdClient) (*backuppb.StreamBackupTaskInfo, error) {
	ns := backup.Namespace
	name := backup.Name

	key := path.Join(streamKeyPrefix, taskInfoPath, name)
	kvs, err := etcdCli.Get(key, true)
	if err != nil {
		klog.Errorf("get log backup %s/%s tikv task error %v", ns, name, err)
		return nil, err
	}
	if len(kvs) < 1 {
		klog.Infof("log backup %s/%s tikv task not found", ns, name)
		return nil, nil
	}

	var taskInfo backuppb.StreamBackupTaskInfo
	err = proto.Unmarshal(kvs[0].Value, &taskInfo)
	if err != nil {
		klog.Errorf("log backup %s/%s tikv task invalid binary", ns, name)
		// return nil, errors.Annotatef(err, "invalid binary presentation of task info (name = %s)", name)
		return nil, err
	}

	klog.Infof("get taskinfo %v", taskInfo)
	return &taskInfo, nil
}

func isTikvTaskPause(backup *v1alpha1.Backup, etcdCli pdapi.PDEtcdClient) (bool, error) {
	ns := backup.Namespace
	name := backup.Name
	key := path.Join(streamKeyPrefix, taskPausePath, name)
	klog.Infof("log backup %s/%s tikv task key %s", ns, name, key)

	kvs, err := etcdCli.Get(key, true)
	if err != nil {
		klog.Errorf("get log backup %s/%s tikv task error %v", ns, name, err)
		return false, err
	}
	if len(kvs) < 1 {
		klog.Infof("log backup %s/%s tikv task is not pause", ns, name)
		return false, nil
	}

	klog.Infof("log backup %s/%s tikv task is pause", ns, name)
	return true, nil
}

func getTikvTaskError(backup *v1alpha1.Backup, etcdCli pdapi.PDEtcdClient) (map[uint64]backuppb.StreamBackupError, error) {
	ns := backup.Namespace
	name := backup.Name

	key := strings.TrimSuffix(path.Join(streamKeyPrefix, taskLastErrorPath, name), "/") + "/"
	klog.Infof("log backup %s/%s tikv task key %s", ns, name, key)

	kvs, err := etcdCli.Get(key, true)
	if err != nil {
		klog.Errorf("get log backup %s/%s tikv task error %v", ns, name, err)
		return nil, err
	}
	if len(kvs) < 1 {
		klog.Infof("log backup %s/%s tikv task is not error", ns, name)
		return nil, nil
	}

	// copy from https://github.com/pingcap/tidb/blob/a768cd3d364b58d81637028f3d6fde24ec53be8c/br/pkg/streamhelper/client.go#L413
	storeToError := map[uint64]backuppb.StreamBackupError{}
	for _, r := range kvs {
		storeIDStr := strings.TrimPrefix(string(r.Key), key)
		storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
		if err != nil {
			return nil, perrors.Annotatef(err, "failed to parse the store ID string %s", storeIDStr)
		}
		var lastErr backuppb.StreamBackupError
		if err := proto.Unmarshal(r.Value, &lastErr); err != nil {
			return nil, perrors.Annotatef(err, "failed to parse wire encoding for store %d", storeID)
		}
		storeToError[storeID] = lastErr
	}
	return storeToError, nil
}

func getTikvTaskCheckpointTS(backup *v1alpha1.Backup, etcdCli pdapi.PDEtcdClient) (*string, error) {
	ns := backup.Namespace
	name := backup.Name

	key := path.Join(streamKeyPrefix, taskCheckpointPath, name)
	klog.Infof("log backup %s/%s checkpointTS key %s", ns, name, key)

	kvs, err := etcdCli.Get(key, true)
	if err != nil {
		klog.Errorf("get log backup %s/%s checkpointTS error %v", ns, name, err)
		return nil, err
	}
	if len(kvs) < 1 {
		klog.Errorf("log backup %s/%s checkpointTS not found", ns, name)
		return nil, nil
	}
	ckTS := strconv.FormatUint(binary.BigEndian.Uint64(kvs[0].Value), 10)
	klog.Infof("get log backup %s/%s checkpointTS %s ", ns, name, ckTS)
	return &ckTS, nil
}
