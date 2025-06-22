# build image
docker build --no-cache --platform linux/amd64 -t jackcky/superpricewatchdog:v0 -f docker/Dockerfile .

# tag image
docker tag jackcky/superpricewatchdog:v0 jackcky/superpricewatchdog:latest

# push to Docker Hub
docker push jackcky/superpricewatchdog:latest

# run container
docker run --name superpricewatchdog --platform linux/amd64 --env-file config/.env -p 5000:5000 jackcky/superpricewatchdog:latest
