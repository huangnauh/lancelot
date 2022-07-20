package trace

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/opentracing/opentracing-go"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"sourcegraph.com/sourcegraph/appdash"
	static "sourcegraph.com/sourcegraph/appdash-data"
	traceImpl "sourcegraph.com/sourcegraph/appdash/opentracing"
	"sourcegraph.com/sourcegraph/appdash/traceapp"
)

var Store *appdash.MemoryStore = appdash.NewMemoryStore()

type sortByStartTime []*appdash.Trace

func getStartTime(trace *appdash.Trace) (t time.Time) {
	if e, err := trace.TimespanEvent(); err == nil {
		t = e.Start()
	}
	return
}

func (t sortByStartTime) Len() int { return len(t) }
func (t sortByStartTime) Less(i, j int) bool {
	return getStartTime(t[j]).After(getStartTime(t[i]))
}
func (t sortByStartTime) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

func GetTraces() ([]string, error) {
	traces, err := Store.Traces(appdash.TracesOpts{})
	if err != nil {
		return nil, err
	}
	if len(traces) == 0 {
		return nil, nil
	}
	ids := make([]string, len(traces))
	sort.Sort(sortByStartTime(traces))
	for _, trace := range traces {
		ids = append(ids, trace.ID.String())
	}
	return ids, nil
}

func NewTrace() opentracing.Tracer {
	return traceImpl.NewTracer(Store)
}

func Router(router *mux.Router, cfg *config.Config) error {
	host := cfg.Host
	if host == "" || host == "0.0.0.0" {
		host = "localhost"
	}
	baseURL, err := url.Parse(fmt.Sprintf(
		"http://%s:%d/web/trace/", host, cfg.HttpPort))
	if err != nil {
		return err
	}
	router.PathPrefix("/web/trace/static/").Handler(
		http.StripPrefix("/web/trace/static/", http.FileServer(static.Data)))

	r := router.PathPrefix("/web/trace/").Subrouter()
	traceRouter := traceapp.NewRouter(r)
	app, err := traceapp.New(traceRouter, baseURL)
	if err != nil {
		return err
	}
	app.Store = &appdash.RecentStore{
		MinEvictAge: time.Hour,
		DeleteStore: Store,
	}
	app.Queryer = Store
	router.Handle("/trace/", app)
	return nil
}
