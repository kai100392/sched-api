
#curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dev.cdh-az-sched-n.caf.mccapp.com/patient-like-me" -d @test.json 

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" "http://localhost:8000/patient-like-me" -d @test.json 
