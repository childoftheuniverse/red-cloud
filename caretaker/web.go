package main

import (
	"context"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/childoftheuniverse/red-cloud"
	"github.com/childoftheuniverse/red-cloud/common"
	"go.opencensus.io/trace"
)

var funcMap = map[string]interface{}{
	"bytes": common.PrettyBytes,
	"since": common.TimeSince,
}

var nodeTemplate = template.Must(
	template.New("statuspage").Funcs(funcMap).Parse(`
<!DOCTYPE html PUBLIC>
<html xmlns="http://www.w3.org/1999/xhtml">
 <head>
  <title>Status of {{ .Instance }} node {{ .Node.Address }}</title>
  <link rel="stylesheet" type="text/css" href="{{ .BootstrapCSSPath }}"
    integrity="{{ .BootstrapCSSHash }}" crossorigin="anonymous" />
 </head>
 <body>
  <div class="container">
   <h1>Status of red-cloud node {{ .Node.Address }}
      <small>{{ .Instance }}</small></h1>
   <dl>
    <dt>Address</dt>
    <dd><a href="http://{{ .Node.StatusAddress }}">{{ .Node.Address }}</a></td>
    <dt>Status</dt>
    <dd>{{ if .Node.IsHealthy }}Healthy{{ else }}Unhealthy{{ end }}</dd>
    <dt>Heap usage</dt>
    <dd title="{{ .Node.HeapUsage }}">{{ bytes .Node.HeapUsage }}</dd>
    <dt>Last check</dt>
    <dd title="{{ .Node.LastCheck }}">{{ since .Node.LastCheck }} ago</dd>
   </dl>
   <h2>Assigned tablets</h2>
{{ range $table, $tablets := .Tablets }}
   <h3><a href="/table?id={{ urlquery $table }}">{{ $table }}</a></h3>
   <table class="table table-striped">
    <thead>
     <tr><th>Start key</th><th>End key</th></tr>
    </thead>
    <tbody>
{{ range $tablets }}
     <tr><td>{{ .StartKey }}</td><td>{{ .EndKey }}</td></tr>
{{ end }}
    </tbody>
   </table>
{{ end }}
  </div>
 </body>
</html>
`))

type nodeStatusData struct {
	BootstrapCSSPath string
	BootstrapCSSHash string
	Instance         string
	Node             *DataNode
	Tablets          map[string][]*common.KeyRange
}

var tableStatusTemplate = template.Must(
	template.New("statuspage").Funcs(funcMap).Parse(`
<!DOCTYPE html PUBLIC>
<html xmlns="http://www.w3.org/1999/xhtml">
 <head>
  <title>Tablets of red-cloud://{{ .Instance }}/{{ .Table }}</title>
  <link rel="stylesheet" type="text/css" href="{{ .BootstrapCSSPath }}"
    integrity="{{ .BootstrapCSSHash }}" crossorigin="anonymous" />
 </head>
 <body>
  <div class="container">
   <h1>Tablets of red-cloud://{{ .Instance }}/{{ .Table }}
      <small>{{ .Instance }}</small></h1>
   <table class="table table-striped">
    <thead>
     <tr><th>Start key</th><th>End key</th><th>Node</th></tr>
    </thead>
    <tbody>
{{ range .Tablets }}
     <tr>
      <td>{{ .StartKey }}</td>
      <td>{{ .EndKey }}</td>
      <td><a href="/node?id={{ urlquery .Host }}:{{ .Port }}">{{ .Host }}:{{ .Port }}</a></td>
     </tr>
{{ end }}
    </tbody>
   </table>
  </div>
 </body>
</html>
`))

type tableStatusData struct {
	BootstrapCSSPath string
	BootstrapCSSHash string
	Instance         string
	Table            string
	Tablets          []*redcloud.ServerTabletMetadata
}

var mainStatusTemplate = template.Must(
	template.New("statuspage").Funcs(funcMap).Parse(`
<!DOCTYPE html PUBLIC>
<html xmlns="http://www.w3.org/1999/xhtml">
 <head>
  <title>Status of {{.Instance}}</title>
  <link rel="stylesheet" type="text/css" href="{{ .BootstrapCSSPath }}"
    integrity="{{ .BootstrapCSSHash }}" crossorigin="anonymous" />
 </head>
 <body>
  <div class="container">
   <h1>Instance Status <small>{{.Instance}}</small></h1>
   <div class="row">
    <div class="col-md-6">
     <h2>Active nodes</h2>
     <table class="table table-striped">
      <thead>
       <tr><th>Node address</th><th>Heap Usage</th><th>Last seen</th></tr>
      </thead>
      <tbody>
{{range .Alive}}
       <tr>
        <td><a href="/node?id={{ urlquery .Address }}">{{ .Address }}</a></td>
        <td title="{{ .HeapUsage }}">{{ bytes .HeapUsage }}</td>
        <td title="{{ .LastCheck }}">{{ since .LastCheck }} ago</td>
       </tr>
{{end}}
      </tbody>
     </table>
    </div>
    <div class="col-md-6">
     <h2>Missing nodes</h2>
     <table class="table table-striped">
      <thead>
       <tr><th>Node address</th><th>Heap Usage</th><th>Last seen</th></tr>
      </thead>
      <tbody>
{{range .Dead}}
       <tr>
        <td><a href="/node?id={{ urlquery .Address }}">{{ .Address }}</a></td>
        <td title="{{ .HeapUsage }}">{{ bytes .HeapUsage }}</td>
        <td title="{{ .LastCheck }}">{{ since .LastCheck }} ago</td>
       </tr>
{{end}}
      </tbody>
     </table>
    </div>
   </div>
   <h2>Known tables</h2>
   <div class="row">
    <div class="col-md-12">
     <table class="table table-striped">
      <thead>
       <tr><th>Table name</th><th>Tablets</th></tr>
      </thead>
      <tbody>
{{range .Tables}}
       <tr>
        <td><a href="/table?id={{ urlquery .Name }}">{{ .Name }}</a></td>
        <td><a href="/table?id={{ urlquery .Name }}">{{ .Tablet | len }}</a></td>
       </tr>
{{end}}
      </tbody>
     </table>
    </div>
   </div>
  </div>
 </body>
</html>
`))

/*
mainStatusData provides data for the mainStatusTemplate.
*/
type mainStatusData struct {
	BootstrapCSSPath string
	BootstrapCSSHash string
	Instance         string
	Alive            []*DataNode
	Dead             []*DataNode
	Tables           []*redcloud.ServerTableMetadata
}

/*
StatusWebService provides a small amount of information about the current
status of the red-cloud caretaker node as a web interface.
*/
type StatusWebService struct {
	registry         *DataNodeRegistry
	bootstrapCSSPath string
	bootstrapCSSHash string
}

/*
NewStatusWebService creates a new status web service object which can be
exported via HTTP.
*/
func NewStatusWebService(
	bootstrapCSSPath, bootstrapCSSHash string,
	registry *DataNodeRegistry) *StatusWebService {
	return &StatusWebService{
		registry:         registry,
		bootstrapCSSPath: bootstrapCSSPath,
		bootstrapCSSHash: bootstrapCSSHash,
	}
}

/*
ServeHTTP serves status information about the state of the caretaker node
as an HTML page over HTTP.
*/
func (s *StatusWebService) ServeHTTP(
	rw http.ResponseWriter, req *http.Request) {
	var parentCtx context.Context
	var ctx context.Context
	var cancel context.CancelFunc
	var span *trace.Span

	ctx, cancel = context.WithTimeout(req.Context(), 20*time.Second)
	defer cancel()

	ctx, span = trace.StartSpan(
		parentCtx, "red-cloud.StatusWebService/ServeHTTP")
	defer span.End()

	span.AddAttributes(
		trace.StringAttribute("path", req.URL.Path))

	if req.URL.Path == "/node" {
		var nodeData = nodeStatusData{
			BootstrapCSSPath: s.bootstrapCSSPath,
			BootstrapCSSHash: s.bootstrapCSSHash,
			Instance:         s.registry.Instance(),
		}
		var err error

		if nodeData.Node = s.registry.GetNodeByAddress(
			ctx, req.FormValue("id")); nodeData.Node == nil {
			span.Annotate(
				[]trace.Attribute{
					trace.StringAttribute("target-address", req.FormValue("id")),
				}, "Node not found")
			http.Error(rw, "No such node", http.StatusNotFound)
			return
		}

		if nodeData.Tablets, err = s.registry.GetNodeTablets(
			ctx, req.FormValue("id")); err != nil {
			log.Print("Error determining tablets held by ",
				req.FormValue("id"), ": ", err)
		}

		if err = nodeTemplate.Execute(rw, nodeData); err != nil {
			log.Print("Error sending node status page: ", err)
		}
	} else if req.URL.Path == "/table" {
		var tableData = tableStatusData{
			BootstrapCSSPath: s.bootstrapCSSPath,
			BootstrapCSSHash: s.bootstrapCSSHash,
			Instance:         s.registry.Instance(),
			Table:            req.FormValue("id"),
		}
		var err error

		if tableData.Tablets, err = s.registry.GetTablets(
			ctx, req.FormValue("id")); err != nil {
			span.Annotate(
				[]trace.Attribute{
					trace.StringAttribute("table", req.FormValue("id")),
				}, "Error requesting table metadata")
			log.Print("Error determining the tablets in table ",
				req.FormValue("id"), ": ", err)
		}

		if err = tableStatusTemplate.Execute(rw, tableData); err != nil {
			log.Print("Error sending table status page: ", err)
		}
	} else {
		var mainData = mainStatusData{
			BootstrapCSSPath: s.bootstrapCSSPath,
			BootstrapCSSHash: s.bootstrapCSSHash,
			Instance:         s.registry.Instance(),
		}
		var mds map[string]*redcloud.ServerTableMetadata
		var md *redcloud.ServerTableMetadata
		var err error

		mainData.Alive, mainData.Dead = s.registry.GetNodeLists(ctx)

		if mds, err = s.registry.GetTableList(ctx); err != nil {
			log.Print("Error fetching table list: ", err)
		}

		for _, md = range mds {
			mainData.Tables = append(mainData.Tables, md)
		}

		if err = mainStatusTemplate.Execute(rw, mainData); err != nil {
			log.Print("Error sending status page: ", err)
		}
	}
}
