package netstorage

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
)

func TestParseStorageNodeLabelIndexFailure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		if _, err := parseStorageNodeLabelIndex(s); err == nil {
			t.Fatalf("expecting non-nil error for -storageNodeLabelIndex=%q", s)
		}
	}

	// missing `=`
	f(`region`)

	// empty label name
	f(`=eu-1`)

	// empty label value
	f(`region=`)
	f(`region=eu-1^^`)
	f(`region=^^eu-1`)

	// duplicate label value
	f(`region=eu-1^^eu-1`)
}

func TestStorageNodeLabelIndexMayMatch(t *testing.T) {
	f := func(labelIndex, query string, resultExpected bool) {
		t.Helper()
		li, err := parseStorageNodeLabelIndex(labelIndex)
		if err != nil {
			t.Fatalf("cannot parse -storageNodeLabelIndex=%q: %s", labelIndex, err)
		}
		tfss, err := searchutil.ParseMetricSelector(query)
		if err != nil {
			t.Fatalf("cannot parse query %q: %s", query, err)
		}
		result := li.mayMatch(newQueryFilters(tfss))
		if result != resultExpected {
			t.Fatalf("unexpected result for -storageNodeLabelIndex=%s and query %s; got %v; want %v", labelIndex, query, result, resultExpected)
		}
	}

	// the query has no filters for the indexed label
	f(`region=eu-1`, `up`, true)
	f(`region=eu-1`, `up{job="foo"}`, true)

	// `=` query filter
	f(`region=eu-1`, `up{region="eu-1"}`, true)
	f(`region=eu-1`, `up{region="eu-2"}`, false)
	f(`region=eu-1`, `up{region=""}`, false)

	// `!=` query filter
	f(`region=eu-1`, `up{region!="eu-2"}`, true)
	f(`region=eu-1`, `up{region!="eu-1"}`, false)

	// `=~` query filter
	f(`region=eu-1`, `up{region=~"eu-.*"}`, true)
	f(`region=eu-1`, `up{region=~"eu-1|us-1"}`, true)
	f(`region=eu-1`, `up{region=~"us-.*"}`, false)
	f(`region=eu-1`, `up{region=~"eu"}`, false)

	// `!~` query filter
	f(`region=eu-1`, `up{region!~"us-.*"}`, true)
	f(`region=eu-1`, `up{region!~"eu-.*"}`, false)

	// multiple query filters for the indexed label
	f(`region=eu-1`, `up{region=~"eu-.*",region!="eu-1"}`, false)
	f(`region=eu-1`, `up{region=~"eu-.*",region!="eu-2"}`, true)

	// metric name
	f(`__name__=up`, `up`, true)
	f(`__name__=up`, `down`, false)

	// multiple values at the label index
	f(`region=us-east^^us-west`, `up{region="us-west"}`, true)
	f(`region=us-east^^us-west`, `up{region=~"eu-.*"}`, false)
	f(`region=us-east^^us-west`, `up{region!~"us-.*"}`, false)
	f(`region=us-east^^us-west`, `up{region!="us-east"}`, true)
	f(`region=us-east^^us-west`, `up{region=~"us-.*",region!="us-east"}`, true)
	f(`region=us-east^^us-west`, `up{region=~"us-.*",region!~"us-(east|west)"}`, false)

	// `or` at the query
	f(`region=eu-1`, `{region="us-1" or region="eu-1"}`, true)
	f(`region=eu-1`, `{region="us-1" or region="us-2"}`, false)
	f(`region=eu-1`, `{region="us-1" or job="foo"}`, true)
}

func TestStorageNodesRequestFilteredCollectResults(t *testing.T) {
	snb := initStorageNodes([]string{"vmselect-eu-a", "vmselect-eu-b", "vmselect-us"}, []string{"region=eu-1", "region=eu-1", "region=us-east"})
	defer mustStopStorageNodes(snb)

	tfss, err := searchutil.ParseMetricSelector(`up{region="eu-1"}`)
	if err != nil {
		t.Fatalf("cannot parse query: %s", err)
	}
	partialResultsCounter := metrics.NewSet().NewCounter("partial_results_total")

	// vmselect-us is skipped, so the query is sent only to vmselect-eu-a and vmselect-eu-b.
	f := func(failedNodes []string, isPartialExpected bool, statusCodeExpected int) {
		t.Helper()
		snr := startStorageNodesRequestFiltered(nil, snb.sns, tfss, false, func(_ *querytracer.Tracer, _ uint, sn *storageNode) any {
			for _, addr := range failedNodes {
				if strings.HasPrefix(sn.connPool.Addr(), addr+":") {
					return errors.New("connection refused")
				}
			}
			return nil
		})
		isPartial, err := snr.collectResults(partialResultsCounter, func(result any) error {
			err, _ := result.(error)
			return err
		})
		if isPartial != isPartialExpected {
			t.Fatalf("unexpected isPartial; got %v; want %v", isPartial, isPartialExpected)
		}
		if statusCodeExpected == 0 {
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			return
		}
		var esc *httpserver.ErrorWithStatusCode
		if !errors.As(err, &esc) {
			t.Fatalf("expecting error with status code %d; got %v", statusCodeExpected, err)
		}
		if esc.StatusCode != statusCodeExpected {
			t.Fatalf("unexpected status code; got %d; want %d", esc.StatusCode, statusCodeExpected)
		}
	}

	// all the queried nodes return responses
	f(nil, false, 0)

	// a part of queried nodes is unavailable
	f([]string{"vmselect-eu-a"}, true, 0)

	// all the queried nodes are unavailable, so there is no data to return as partial response
	f([]string{"vmselect-eu-a", "vmselect-eu-b"}, false, http.StatusServiceUnavailable)
}
