
#curl -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dev.cdh-az-sched-n.caf.mccapp.com/schedule/12345678" -d @test.json 

curl -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" "http://localhost:8000/schedule/12345678"
