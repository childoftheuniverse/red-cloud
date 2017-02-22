package main

import (
	"html/template"
	"log"
	"net/http"

	"github.com/childoftheuniverse/red-cloud/common"
)

var funcMap = map[string]interface{}{
	"bytes": common.PrettyBytes,
	"since": common.TimeSince,
}

var mainStatusTemplate = template.Must(
	template.New("statuspage").Funcs(funcMap).Parse(`
<!DOCTYPE html PUBLIC>
<html xmlns="http://www.w3.org/1999/xhtml">
 <head>
  <title>{{.Instance}} Data Node Status</title>
  <link rel="stylesheet" type="text/css" href="{{ .BootstrapCSSPath }}"
    integrity="{{ .BootstrapCSSHash }}" crossorigin="anonymous" />
 </head>
 <body>
  <div class="container">
   <h1>Data Node Status <small>{{.Instance}}</small></h1>
   <div class="row">
    <div class="col-md-12">
     <h2>Loaded Tablets</h2>
     <table class="table table-striped">
      <thead>
       <tr><th>Table</th><th>Start Key</th><th>End Key</th></tr>
      </thead>
      <tbody>
{{range .Tablets}}
       <tr>
        <td>{{ .Table }}</td>
        <td>{{ .KeyRange.StartKey }}</td>
        <td>{{ .KeyRange.EndKey }}</td>
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

type tableKeyRange struct {
	Table    string
	KeyRange *common.KeyRange
}

type mainStatusData struct {
	BootstrapCSSPath string
	BootstrapCSSHash string
	Instance         string
	Tablets          []*tableKeyRange
}

/*
StatusWebService provides a small amount of information about the current
status of the red-cloud data node as a web interface.
*/
type StatusWebService struct {
	registry         *ServingRangeRegistry
	bootstrapCSSPath string
	bootstrapCSSHash string
}

/*
NewStatusWebService creates a new status web service object which can be
exported via HTTP.
*/
func NewStatusWebService(
	bootstrapCSSPath, bootstrapCSSHash string,
	registry *ServingRangeRegistry) *StatusWebService {
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
	var mainData = mainStatusData{
		BootstrapCSSPath: s.bootstrapCSSPath,
		BootstrapCSSHash: s.bootstrapCSSHash,
		Instance:         s.registry.Instance(),
	}
	var ranges []*common.KeyRange
	var table string
	var err error

	for table, ranges = range s.registry.GetRanges() {
		var rn *common.KeyRange

		for _, rn = range ranges {
			mainData.Tablets = append(mainData.Tablets, &tableKeyRange{
				Table:    table,
				KeyRange: rn,
			})
		}
	}

	if err = mainStatusTemplate.Execute(rw, mainData); err != nil {
		log.Print("Error sending status page: ", err)
	}
}
