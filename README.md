# Introduction 
AZ Scheduling API

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Local env setup (mac)
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r src/requirements.txt

# Build container and pushing to GCP
From the top level directory run the following commands to test the service:

```
gcloud auth login
gcloud auth application-default login
gcloud auth login --update-adc
gcloud auth configure-docker us-central1-docker.pkg.dev
docker build --no-cache -t us-central1-docker.pkg.dev/cdh-az-sched-n-328641622107/artifact-repository/sched-api:<your-label> ./
docker push us-central1-docker.pkg.dev/cdh-az-sched-n-328641622107/artifact-repository/sched-api:<your-label>
```

Adjust "your-label" to something that you can easily identify the image version in artificat registry.

In Cloud Run in CAF Dev, use the [Edit & Deploy New Revision button] to test your new build

After this deploys, the front end app in CAF Dev will be interacting with the new `sched-api`: dev.cdh-az-sched-n.caf.mccapp.com

# Test
gcloud auth application-default login
fastapi dev src/server.py

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)