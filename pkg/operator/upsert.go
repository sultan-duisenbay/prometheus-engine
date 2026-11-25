// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"context"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// upsertConfigMap creates or updates the target ConfigMap in place, preserving
// external metadata. Only Labels, Annotations, Data, and BinaryData are reconciled from desired.
func upsertConfigMap(ctx context.Context, c client.Client, desired *corev1.ConfigMap) error {
	cm := &corev1.ConfigMap{
		Namespace: desired.Namespace,
		Name:      desired.Name,
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, cm, func() error {
		cm.Labels = mergeStringMap(cm.Labels, desired.Labels)
		cm.Annotations = mergeStringMap(cm.Annotations, desired.Annotations)
		cm.Data = maps.Clone(desired.Data)
		cm.BinaryData = maps.Clone(desired.BinaryData)
		return nil
	})
	return err
}

// upsertSecret creates or updates the target Secret in place, preserving
// external metadata. Only Labels, Annotations, and Data are reconciled from desired.
func upsertSecret(ctx context.Context, c client.Client, desired *corev1.Secret) error {
	secret := &corev1.Secret{
		Namespace: desired.Namespace,
		Name:      desired.Name,
	}
	_, err := controllerutil.CreateOrUpdate(ctx, c, secret, func() error {
		secret.Labels = mergeStringMap(secret.Labels, desired.Labels)
		secret.Annotations = mergeStringMap(secret.Annotations, desired.Annotations)
		secret.Data = maps.Clone(desired.Data)
		return nil
	})
	return err
}

func mergeStringMap(base, overrides map[string]string) map[string]string {
	if len(base) == 0 && len(overrides) == 0 {
		return base
	}
	out := make(map[string]string, len(base)+len(overrides))
	maps.Copy(out, base)
	maps.Copy(out, overrides)
	return out
}
