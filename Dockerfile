FROM python:3.10-alpine
WORKDIR /trainingportal_janitor
# copy pre-built packages to this image
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
ENTRYPOINT ["python3", "-m", "trainingportal_janitor"]