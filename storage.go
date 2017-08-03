package tprstorage

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	"github.com/giantswarm/operatorkit/tpr"
	"k8s.io/apimachinery/pkg/api/errors"
	apismeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	api "k8s.io/client-go/pkg/api/v1"
)

type TPRConfig struct {
	Name, Version, Description string
}

type TPOConfig struct {
	Name, Namespace string
}

type Config struct {
	// Dependencies.

	K8sClient kubernetes.Interface
	Logger    micrologger.Logger

	// Settings.

	// TPR is the third party resource where data objects are stored.
	TPR TPRConfig

	// TPOName is the third party object used to store data. This object
	// will be created inside a third party resource specified by TPR. If
	// the object already exists it will be reused. It is safe to run
	// multiple Storage instances using the same TPO.
	TPO TPOConfig
}

func DefaultConfig() Config {
	return Config{
		// Dependencies.

		K8sClient: nil, // Required.
		Logger:    nil, // Required.

		// Settings.

		TPR: TPRConfig{
			Name:        "tpr-storage.giantswarm.io",
			Version:     "v1",
			Description: "Storage data managed by github.com/giantswarm/tprstorage",
		},

		TPO: TPOConfig{
			Name:      "", // Required.
			Namespace: "giantswarm",
		},
	}
}

type Storage struct {
	logger micrologger.Logger
	logctx []interface{}

	k8sClient kubernetes.Interface
	tpr       *tpr.TPR

	tpoEndpoint     string
	tpoListEndpoint string
}

func New(ctx context.Context, config Config) (*Storage, error) {
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.K8sClient is nil")
	}
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.Logger is nil")
	}
	if config.TPR.Name == "" {
		return nil, microerror.Maskf(invalidConfigError, "config.TPR.Name is empty")
	}
	if config.TPR.Version == "" {
		return nil, microerror.Maskf(invalidConfigError, "config.TPR.Version is empty")
	}
	// config.TPR.Description is OK to be empty.
	if config.TPO.Name == "" {
		return nil, microerror.Maskf(invalidConfigError, "config.TPO.Name is empty")
	}
	if config.TPO.Namespace == "" {
		config.TPO.Namespace = "default"
	}

	var newTPR *tpr.TPR
	{
		c := tpr.DefaultConfig()

		c.Logger = config.Logger

		c.K8sClient = config.K8sClient

		c.Name = config.TPR.Name
		c.Version = config.TPR.Version
		c.Description = config.TPR.Description

		var err error

		newTPR, err = tpr.New(c)
		if err != nil {
			return nil, microerror.Mask(err)
		}
	}

	s := &Storage{
		k8sClient: config.K8sClient,
		tpr:       newTPR,

		tpoEndpoint:     newTPR.Endpoint(config.TPO.Namespace) + "/" + config.TPO.Name,
		tpoListEndpoint: newTPR.Endpoint(config.TPO.Namespace),

		logger: config.Logger.With(
			"tprName", config.TPR.Name,
			"tprVersion", config.TPR.Version,
			"tpoName", config.TPO.Name,
			"tpoNamespace", config.TPO.Namespace,
		),
	}

	// TODO extract init func

	// Create TPR resource.
	{
		err := s.tpr.CreateAndWait()
		if tpr.IsAlreadyExists(err) {
			s.logger.Log("debug", "TPR already exists")
		} else if err != nil {
			return nil, microerror.Mask(err)
		} else {
			s.logger.Log("debug", "TPR created")
		}
	}

	// Create namespace.
	{
		ns := api.Namespace{
			ObjectMeta: apismeta.ObjectMeta{
				Name:      config.TPO.Namespace,
				Namespace: config.TPO.Namespace,
				// TODO think about labels
			},
		}
		_, err := s.k8sClient.CoreV1().Namespaces().Create(&ns)
		if errors.IsAlreadyExists(err) {
			s.logger.Log("debug", "namespace "+ns.Name+" already exists")
		} else if err != nil {
			return nil, microerror.Maskf(err, "creating namespace %#v", ns)
		} else {
			s.logger.Log("debug", "namespace "+ns.Name+" created")
		}
	}

	// Create TPO.
	{
		tpo := customObject{
			TypeMeta: apismeta.TypeMeta{
				Kind:       s.tpr.Kind(),
				APIVersion: s.tpr.APIVersion(),
			},
			ObjectMeta: apismeta.ObjectMeta{
				Name:      config.TPO.Name,
				Namespace: config.TPO.Namespace,
				Annotations: map[string]string{
					"storageDoNotOmitempty": "non-empty",
				},
				// TODO think about labels
			},

			// Data must be not empty so patches do not fail.
			Data: map[string]string{},
		}
		body, err := json.Marshal(&tpo)
		if err != nil {
			return nil, microerror.Maskf(err, "marshaling %#v", tpo)
		}
		_, err = s.k8sClient.Core().RESTClient().
			Post().
			Context(ctx).
			AbsPath(s.tpoListEndpoint).
			Body(body).
			DoRaw()
		if errors.IsAlreadyExists(err) {
			s.logger.Log("debug", "TPO "+tpo.Name+" already exists")
		} else if err != nil {
			return nil, microerror.Maskf(err, "creating TPO %#v", tpo)
		} else {
			s.logger.Log("debug", "TPO "+tpo.Name+" created")
		}
	}

	return s, nil
}

func (s *Storage) Create(ctx context.Context, key, value string) error {
	err := s.Put(ctx, key, value)
	if err != nil {
		microerror.Mask(err)
	}
	return nil
}

func (s *Storage) Put(ctx context.Context, key, value string) error {
	var body []byte
	{
		v := struct {
			Data map[string]string `json:"data"`
		}{
			Data: map[string]string{
				key: value,
			},
		}

		var err error
		body, err = json.Marshal(&v)
		if err != nil {
			return microerror.Maskf(err, "marshaling %#v", v)
		}
	}

	_, err := s.k8sClient.Core().RESTClient().
		Patch(types.MergePatchType).
		Context(ctx).
		AbsPath(s.tpoEndpoint).
		Body(body).
		DoRaw()
	if err != nil {
		return microerror.Maskf(err, "putting key=%s, patch=%s", key, body)
	}

	return nil
}

func (s *Storage) Exists(ctx context.Context, key string) (bool, error) {
	data, err := s.getData(ctx)
	if err != nil {
		return false, microerror.Maskf(err, "checking existence key=%s", key)
	}

	_, ok := data[key]
	return ok, nil
}

func (s *Storage) Search(ctx context.Context, key string) (string, error) {
	data, err := s.getData(ctx)
	if err != nil {
		return "", microerror.Maskf(err, "searching for key=%s", key)
	}

	v, ok := data[key]
	if !ok {
		return "", microerror.Maskf(notFoundError, "searching for key=%s", key)
	}

	return v, nil
}

func (s *Storage) List(ctx context.Context, key string) ([]string, error) {
	data, err := s.getData(ctx)
	if err != nil {
		return nil, microerror.Maskf(err, "listing key=%s", key)
	}

	var list []string

	keyLen := len(key)
	for k, _ := range data {
		if !strings.HasPrefix(k, key) {
			continue
		}

		// k must be exact match or be separated with /.
		// I.e. /foo is under /foo/bar but not under /foobar.
		if len(k) != keyLen && k[keyLen] != '/' {
			continue
		}

		list = append(list, k[keyLen+1:])
	}

	return list, nil
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	var body []byte
	{
		v := struct {
			Data map[string]*string `json:"data"`
		}{
			Data: map[string]*string{
				key: nil,
			},
		}

		var err error
		body, err = json.Marshal(&v)
		if err != nil {
			return microerror.Maskf(err, "marshaling %#v", v)
		}
	}

	_, err := s.k8sClient.Core().RESTClient().
		Patch(types.MergePatchType).
		Context(ctx).
		AbsPath(s.tpoEndpoint).
		Body(body).
		DoRaw()
	if err != nil {
		return microerror.Maskf(err, "deleting value for key=%s, patch=%s", key, body)
	}

	return nil
}

func (s *Storage) getData(ctx context.Context) (map[string]string, error) {
	res, err := s.k8sClient.Core().RESTClient().
		Get().
		Context(ctx).
		AbsPath(s.tpoEndpoint).
		DoRaw()
	if err != nil {
		return nil, microerror.Maskf(err, "get TPO")
	}

	var v customObject
	err = json.Unmarshal(res, &v)
	if err != nil {
		return nil, microerror.Maskf(err, "unmarshal TPO")
	}

	return v.Data, nil
}
