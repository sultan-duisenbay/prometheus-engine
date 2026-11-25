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
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"maps"
	"testing"

	monitoringv1 "github.com/GoogleCloudPlatform/prometheus-engine/pkg/operator/apis/monitoring/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func gunzipBytes(t *testing.T, data []byte) string {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer r.Close()
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

func newTestExternalMeta(ns string) (map[string]string, map[string]string, []metav1.OwnerReference, []string) {
	return map[string]string{
			"tenancy.gke.io/tenant":                 "tenant-a",
			"tenancy.gke.io/access-level":           "tenant",
			"components.tenancy.gke.io/gke-version": "1.37",
		},
		map[string]string{"custom.example.com/keep": "yes"},
		[]metav1.OwnerReference{{
			APIVersion: "v1",
			Kind:       "Namespace",
			Name:       ns,
			UID:        "owner-uid-1",
		}},
		[]string{"example.com/keep-finalizer"}
}

func assertExternalMetaPreserved(t *testing.T, obj client.Object, labels, annotations map[string]string, ownerRefs []metav1.OwnerReference, finalizers []string) {
	t.Helper()
	for k, v := range labels {
		require.Equal(t, v, obj.GetLabels()[k], "label %q on %s", k, obj.GetName())
	}
	for k, v := range annotations {
		require.Equal(t, v, obj.GetAnnotations()[k], "annotation %q on %s", k, obj.GetName())
	}
	require.Equal(t, ownerRefs, obj.GetOwnerReferences(), "ownerReferences on %s", obj.GetName())
	require.Equal(t, finalizers, obj.GetFinalizers(), "finalizers on %s", obj.GetName())
}

func TestUpsertConfigMap(t *testing.T) {
	ctx := t.Context()
	ns := DefaultOperatorNamespace
	extLabels, extAnnots, ownerRefs, finalizers := newTestExternalMeta(ns)
	key := client.ObjectKey{Namespace: ns, Name: NameCollector}

	t.Run("creates missing ConfigMap", func(t *testing.T) {
		c := newFakeClientBuilder().Build()
		desired := &corev1.ConfigMap{
			Namespace:   ns,
			Name:        NameCollector,
			Labels:      map[string]string{LabelAppName: NameCollector},
			Annotations: map[string]string{AnnotationMetricName: componentName},
			Data:        map[string]string{configFilename: "global: {}"},
		}
		require.NoError(t, upsertConfigMap(ctx, c, desired))

		var got corev1.ConfigMap
		require.NoError(t, c.Get(ctx, key, &got))
		require.Equal(t, desired.Labels, got.Labels)
		require.Equal(t, desired.Annotations, got.Annotations)
		require.Equal(t, desired.Data, got.Data)
		require.Empty(t, got.BinaryData)
	})

	t.Run("updates existing preserving metadata, skips no-op, and toggles Data/BinaryData", func(t *testing.T) {
		updateCalls := 0
		existingLabels := maps.Clone(extLabels)
		existingLabels[LabelAppName] = "stale-app"
		existingAnnots := maps.Clone(extAnnots)
		existingAnnots[AnnotationMetricName] = "stale-metric"
		c := newFakeClientBuilder().
			WithObjects(&corev1.ConfigMap{
				Namespace:       ns,
				Name:            NameCollector,
				Labels:          existingLabels,
				Annotations:     existingAnnots,
				OwnerReferences: ownerRefs,
				Finalizers:      finalizers,
				Data:            map[string]string{"stale.yaml": "old"},
				BinaryData:      map[string][]byte{"old.gz": []byte("stale")},
			}).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					updateCalls++
					return cl.Update(ctx, obj, opts...)
				},
			}).
			Build()

		desired := &corev1.ConfigMap{
			Namespace:   ns,
			Name:        NameCollector,
			Labels:      map[string]string{LabelAppName: NameCollector},
			Annotations: map[string]string{AnnotationMetricName: componentName},
			Data:        map[string]string{configFilename: "new-config"},
		}
		require.NoError(t, upsertConfigMap(ctx, c, desired))
		require.Equal(t, 1, updateCalls)

		var got corev1.ConfigMap
		require.NoError(t, c.Get(ctx, key, &got))
		assertExternalMetaPreserved(t, &got, extLabels, extAnnots, ownerRefs, finalizers)
		require.Equal(t, NameCollector, got.Labels[LabelAppName])
		require.Equal(t, componentName, got.Annotations[AnnotationMetricName])
		require.Equal(t, map[string]string{configFilename: "new-config"}, got.Data)
		require.Empty(t, got.BinaryData)

		// Identical upsert must not issue an Update.
		require.NoError(t, upsertConfigMap(ctx, c, desired))
		require.Equal(t, 1, updateCalls)

		// Switching to BinaryData clears Data; switching back clears BinaryData.
		desiredGzip := &corev1.ConfigMap{
			Namespace:  ns,
			Name:       NameCollector,
			BinaryData: map[string][]byte{configFilename: []byte("gzip-bytes")},
		}
		require.NoError(t, upsertConfigMap(ctx, c, desiredGzip))
		require.Equal(t, 2, updateCalls)
		got = corev1.ConfigMap{}
		require.NoError(t, c.Get(ctx, key, &got))
		assertExternalMetaPreserved(t, &got, extLabels, extAnnots, ownerRefs, finalizers)
		require.Empty(t, got.Data)
		require.Equal(t, map[string][]byte{configFilename: []byte("gzip-bytes")}, got.BinaryData)

		require.NoError(t, upsertConfigMap(ctx, c, desired))
		require.Equal(t, 3, updateCalls)
		got = corev1.ConfigMap{}
		require.NoError(t, c.Get(ctx, key, &got))
		assertExternalMetaPreserved(t, &got, extLabels, extAnnots, ownerRefs, finalizers)
		require.Equal(t, map[string]string{configFilename: "new-config"}, got.Data)
		require.Empty(t, got.BinaryData)
	})
}

func TestUpsertSecret(t *testing.T) {
	ctx := t.Context()
	ns := DefaultOperatorNamespace
	extLabels, extAnnots, ownerRefs, finalizers := newTestExternalMeta(ns)
	key := client.ObjectKey{Namespace: ns, Name: CollectionSecretName}

	t.Run("creates missing Secret", func(t *testing.T) {
		c := newFakeClientBuilder().Build()
		desired := &corev1.Secret{
			Namespace:   ns,
			Name:        CollectionSecretName,
			Labels:      map[string]string{LabelAppName: NameCollector},
			Annotations: map[string]string{AnnotationMetricName: componentName},
			Data:        map[string][]byte{"cred": []byte("token")},
		}
		require.NoError(t, upsertSecret(ctx, c, desired))

		var got corev1.Secret
		require.NoError(t, c.Get(ctx, key, &got))
		require.Equal(t, desired.Labels, got.Labels)
		require.Equal(t, desired.Annotations, got.Annotations)
		require.Equal(t, desired.Data, got.Data)
	})

	t.Run("updates existing preserving metadata and Type, and skips no-op", func(t *testing.T) {
		updateCalls := 0
		c := newFakeClientBuilder().
			WithObjects(&corev1.Secret{
				Namespace:       ns,
				Name:            CollectionSecretName,
				Labels:          maps.Clone(extLabels),
				Annotations:     maps.Clone(extAnnots),
				OwnerReferences: ownerRefs,
				Finalizers:      finalizers,
				Type:            corev1.SecretTypeOpaque,
				Data:            map[string][]byte{"stale": []byte("old")},
			}).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					updateCalls++
					return cl.Update(ctx, obj, opts...)
				},
			}).
			Build()

		desired := &corev1.Secret{
			Namespace:   ns,
			Name:        CollectionSecretName,
			Labels:      map[string]string{LabelAppName: NameCollector},
			Annotations: map[string]string{AnnotationMetricName: componentName},
			Data:        map[string][]byte{"cred": []byte("new-token")},
		}
		require.NoError(t, upsertSecret(ctx, c, desired))
		require.Equal(t, 1, updateCalls)

		var got corev1.Secret
		require.NoError(t, c.Get(ctx, key, &got))
		require.Equal(t, corev1.SecretTypeOpaque, got.Type)
		assertExternalMetaPreserved(t, &got, extLabels, extAnnots, ownerRefs, finalizers)
		require.Equal(t, NameCollector, got.Labels[LabelAppName])
		require.Equal(t, componentName, got.Annotations[AnnotationMetricName])
		require.Equal(t, map[string][]byte{"cred": []byte("new-token")}, got.Data)

		require.NoError(t, upsertSecret(ctx, c, desired))
		require.Equal(t, 1, updateCalls)
	})
}

func TestReconcilersPreserveExternalMetadataAndCompression(t *testing.T) {
	ctx := t.Context()
	opts := Options{
		ProjectID:         "test-project",
		Location:          "us-central1-c",
		Cluster:           "test-cluster",
		OperatorNamespace: DefaultOperatorNamespace,
		PublicNamespace:   DefaultPublicNamespace,
	}
	extLabels, extAnnots, ownerRefs, finalizers := newTestExternalMeta(opts.OperatorNamespace)

	precreatedCM := func(name string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			Namespace:       opts.OperatorNamespace,
			Name:            name,
			Labels:          maps.Clone(extLabels),
			Annotations:     maps.Clone(extAnnots),
			OwnerReferences: append([]metav1.OwnerReference(nil), ownerRefs...),
			Finalizers:      append([]string(nil), finalizers...),
		}
	}
	precreatedSecret := func(name string) *corev1.Secret {
		return &corev1.Secret{
			Namespace:       opts.OperatorNamespace,
			Name:            name,
			Labels:          maps.Clone(extLabels),
			Annotations:     maps.Clone(extAnnots),
			OwnerReferences: append([]metav1.OwnerReference(nil), ownerRefs...),
			Finalizers:      append([]string(nil), finalizers...),
		}
	}

	ruleObj := &monitoringv1.Rules{
		Namespace:  "team-a",
		Name:       "my-rules",
		Generation: 1,
		Spec: monitoringv1.RulesSpec{
			Groups: []monitoringv1.RuleGroup{{
				Name:  "test-group",
				Rules: []monitoringv1.Rule{{Record: "job:up:sum", Expr: "sum(up)"}},
			}},
		},
	}

	c := newFakeClientBuilder().WithObjects(
		ruleObj,
		precreatedCM(NameCollector),
		precreatedCM(nameRulesGenerated),
		precreatedCM(NameRuleEvaluator),
		precreatedSecret(CollectionSecretName),
		precreatedSecret(RulesSecretName),
		precreatedSecret(AlertmanagerSecretName),
	).Build()

	colRec := newCollectionReconciler(c, opts)
	require.NoError(t, colRec.ensureCollectorSecrets(ctx, &monitoringv1.CollectionSpec{}))
	require.NoError(t, colRec.ensureCollectorConfig(ctx, &monitoringv1.CollectionSpec{}, monitoringv1.CompressionNone, nil))

	rulesRec := newRulesReconciler(c, opts)
	require.NoError(t, rulesRec.ensureRuleConfigs(ctx, opts.ProjectID, opts.Location, opts.Cluster, monitoringv1.CompressionNone))

	opRec := newOperatorConfigReconciler(c, opts, false)
	secretData, err := opRec.ensureRuleEvaluatorConfig(ctx, &monitoringv1.RuleEvaluatorSpec{})
	require.NoError(t, err)
	require.NoError(t, opRec.ensureRuleEvaluatorSecrets(ctx, secretData))
	require.NoError(t, opRec.ensureAlertmanagerConfigSecret(ctx, nil))

	for _, name := range []string{NameCollector, nameRulesGenerated, NameRuleEvaluator} {
		var cm corev1.ConfigMap
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: opts.OperatorNamespace, Name: name}, &cm))
		assertExternalMetaPreserved(t, &cm, extLabels, extAnnots, ownerRefs, finalizers)
		require.NotEmpty(t, cm.Data)
	}
	for _, name := range []string{CollectionSecretName, RulesSecretName, AlertmanagerSecretName} {
		var secret corev1.Secret
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: opts.OperatorNamespace, Name: name}, &secret))
		assertExternalMetaPreserved(t, &secret, extLabels, extAnnots, ownerRefs, finalizers)
		require.Equal(t, componentName, secret.Annotations[AnnotationMetricName])
	}

	// Verify gzip round-trip clears inactive Data/BinaryData on collector and rules-generated.
	colKey := client.ObjectKey{Namespace: opts.OperatorNamespace, Name: NameCollector}
	var colCM corev1.ConfigMap
	require.NoError(t, c.Get(ctx, colKey, &colCM))
	origColCfg := colCM.Data[configFilename]

	require.NoError(t, colRec.ensureCollectorConfig(ctx, &monitoringv1.CollectionSpec{}, monitoringv1.CompressionGzip, nil))
	colCM = corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, colKey, &colCM))
	require.Empty(t, colCM.Data)
	require.Equal(t, origColCfg, gunzipBytes(t, colCM.BinaryData[configFilename]))
	assertExternalMetaPreserved(t, &colCM, extLabels, extAnnots, ownerRefs, finalizers)

	require.NoError(t, colRec.ensureCollectorConfig(ctx, &monitoringv1.CollectionSpec{}, monitoringv1.CompressionNone, nil))
	colCM = corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, colKey, &colCM))
	require.Equal(t, map[string]string{configFilename: origColCfg}, colCM.Data)
	require.Empty(t, colCM.BinaryData)

	rulesKey := client.ObjectKey{Namespace: opts.OperatorNamespace, Name: nameRulesGenerated}
	const ruleFile = "rules__team-a__my-rules.yaml"
	var rulesCM corev1.ConfigMap
	require.NoError(t, c.Get(ctx, rulesKey, &rulesCM))
	origRule := rulesCM.Data[ruleFile]

	require.NoError(t, rulesRec.ensureRuleConfigs(ctx, opts.ProjectID, opts.Location, opts.Cluster, monitoringv1.CompressionGzip))
	rulesCM = corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, rulesKey, &rulesCM))
	require.Equal(t, map[string]string{"empty.yaml": ""}, rulesCM.Data)
	require.Equal(t, origRule, gunzipBytes(t, rulesCM.BinaryData[ruleFile]))
	assertExternalMetaPreserved(t, &rulesCM, extLabels, extAnnots, ownerRefs, finalizers)

	require.NoError(t, rulesRec.ensureRuleConfigs(ctx, opts.ProjectID, opts.Location, opts.Cluster, monitoringv1.CompressionNone))
	rulesCM = corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, rulesKey, &rulesCM))
	require.Equal(t, map[string]string{"empty.yaml": "", ruleFile: origRule}, rulesCM.Data)
	require.Empty(t, rulesCM.BinaryData)
}
