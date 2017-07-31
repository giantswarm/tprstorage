package tprstorage

import (
	"context"
	"encoding/json"

	"github.com/giantswarm/microerror"
	"github.com/giantswarm/micrologger"
	"github.com/giantswarm/operatorkit/tpr"
	"k8s.io/apimachinery/pkg/api/errors"
	apismeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	api "k8s.io/client-go/pkg/api/v1"
)

const (
	// TODO think if we want to store as a separate TPO in sing TPR.
	customObjectName = "storage"
	// TODO namespace should be configurable.
	namespace = "pawel"

	pathPrefix = "/data/"
)

type Config struct {
	// Dependencies.

	K8sClient kubernetes.Interface
	Logger    micrologger.Logger

	// Settings.

	TPRName        string
	TPRVersion     string
	TPRDescription string
}

func DefaultConfig() Config {
	return Config{
		// Dependencies.

		K8sClient: nil,
		Logger:    nil,

		// Settings.

		TPRName:        "",
		TPRVersion:     "",
		TPRDescription: "",
	}
}

type Storage struct {
	logger micrologger.Logger
	logctx []interface{}

	k8sClient kubernetes.Interface
	tpr       *tpr.TPR
}

func New(ctx context.Context, config Config) (*Storage, error) {
	if config.K8sClient == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.K8sClient is nil")
	}
	if config.Logger == nil {
		return nil, microerror.Maskf(invalidConfigError, "config.Logger is nil")
	}
	if config.TPRName == "" {
		return nil, microerror.Maskf(invalidConfigError, "config.TPRName is empty")
	}
	if config.TPRVersion == "" {
		return nil, microerror.Maskf(invalidConfigError, "config.TPRVersion is empty")
	}
	// TPRDescription is OK to be empty.

	logctx := []interface{}{
		"tprName", config.TPRName,
		"tprVersion", config.TPRVersion,
	}

	var newTPR *tpr.TPR
	{
		c := tpr.DefaultConfig()

		c.Logger = config.Logger

		c.K8sClient = config.K8sClient

		c.Name = config.TPRName
		c.Version = config.TPRVersion
		c.Description = config.TPRDescription

		var err error

		newTPR, err = tpr.New(c)
		if err != nil {
			return nil, microerror.Mask(err)
		}
	}

	s := &Storage{
		k8sClient: config.K8sClient,
		tpr:       newTPR,

		logger: config.Logger,
		logctx: logctx,
	}

	// Create TPR resource.
	{
		err := s.tpr.CreateAndWait()
		if tpr.IsAlreadyExists(err) {
			s.log("debug", "TPR already exists")
		} else if err != nil {
			return nil, microerror.Mask(err)
		} else {
			s.log("debug", "TPR created")
		}
	}

	// Create namespace.
	{
		ns := api.Namespace{
			ObjectMeta: apismeta.ObjectMeta{
				Name:      namespace,
				Namespace: namespace,
				// TODO think about labels
			},
		}
		_, err := s.k8sClient.CoreV1().Namespaces().Create(&ns)
		if errors.IsAlreadyExists(err) {
			s.log("debug", "namespace "+ns.Name+" already exists")
		} else if err != nil {
			return nil, microerror.Maskf(err, "creating namespace %#v", ns)
		} else {
			s.log("debug", "namespace "+ns.Name+" created")
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
				Name:      customObjectName,
				Namespace: namespace,
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
			AbsPath(s.tpr.Endpoint(namespace)).
			Body(body).
			DoRaw()
		if errors.IsAlreadyExists(err) {
			s.log("debug", "TPO "+tpo.Name+" already exists")
		} else if err != nil {
			return nil, microerror.Maskf(err, "creating TPO %#v", tpo)
		} else {
			s.log("debug", "TPO "+tpo.Name+" created")
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
		AbsPath(s.tpr.Endpoint(namespace) + "/" + customObjectName).
		Body(body).
		DoRaw()
	if err != nil {
		return microerror.Maskf(err, "creating value for key=%s, patch=%s", key, body)
	}

	return nil
}

func (s *Storage) Exists(ctx context.Context, key string) (bool, error) {
	data, err := s.getData(ctx)
	if err != nil {
		return false, microerror.Maskf(err, "Exists")
	}

	_, ok := data[key]
	return ok, nil
}

func (s *Storage) Search(ctx context.Context, key string) (string, error) {
	data, err := s.getData(ctx)
	if err != nil {
		return "", microerror.Maskf(err, "Exists")
	}

	return data[key], nil
}

func (s *Storage) getData(ctx context.Context) (map[string]string, error) {
	res, err := s.k8sClient.Core().RESTClient().
		Get().
		Context(ctx).
		AbsPath(s.tpr.Endpoint(namespace) + "/" + customObjectName).
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

func (s *Storage) log(v ...interface{}) {
	a := make([]interface{}, 0, len(v)+len(s.logctx))
	a = append(a, v...)
	a = append(a, s.logctx...)
	s.logger.Log(a...)
}
