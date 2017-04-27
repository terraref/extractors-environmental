# Environmental Logger JSON to NetCDF converter

This extractor processes environmental logger stream data .JSON files into a netCDF.

_Input_

  - Evaluation is triggered whenever a file is added to a dataset
  - Checks whether the file is an _environmentlogger.json file
  
_Output_

  - The dataset containing the .JSON file will get a corresponding .nc netCDF file
  
### Docker
The Dockerfile included in this directory can be used to launch this extractor in a container.

_Building the Docker image_
```
docker build -f Dockerfile -t terra-ext-envlog2netcdf .
```

_Running the image locally_
```
docker run \
  -p 5672 -p 9000 --add-host="localhost:{LOCAL_IP}" \
  -e RABBITMQ_URI=amqp://{RMQ_USER}:{RMQ_PASSWORD}@localhost:5672/%2f \
  -e RABBITMQ_EXCHANGE=clowder \
  -e REGISTRATION_ENDPOINTS=http://localhost:9000/clowder/api/extractors?key={SECRET_KEY} \
  terra-ext-envlog2netcdf
```
Note that by default RabbitMQ will not allow "guest:guest" access to non-local addresses, which includes Docker. You may need to create an additional local RabbitMQ user for testing.

_Running the image remotely_
```
docker run \
  -e RABBITMQ_URI=amqp://{RMQ_USER}:{RMQ_PASSWORD}@rabbitmq.ncsa.illinois.edu/clowder \
  -e RABBITMQ_EXCHANGE=terra \
  -e REGISTRATION_ENDPOINTS=http://terraref.ncsa.illinosi.edu/clowder//api/extractors?key={SECRET_KEY} \
  terra-ext-envlog2netcdf
```
