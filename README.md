## Specific Provisioner

This REST application mocks a specific provisioner for data product components.

This project uses OpenAPI as standard API specification and the [OpenAPI Generator](https://openapi-generator.tech)

### To compile and generate APIs

```bash
brew install sbt
brew install node
npm install @openapitools/openapi-generator-cli -g
sbt generateCode compile
export WITBOOST_MESH_TOKEN= ... # Ask the project administrator for the token
```

To set permanently the env variable `WITBOOST_MESH_TOKEN`, add the export command in ~/.bashrc

### Configure Intellij
- open the project in IntelliJ
  - accept the "load sbt project" notification on the bottom right corner
  - click on "reload all sbt project" button on the top right window
  - in the package it.agilelab.datamesh.specificprovisioner.server
    - run the server Main class (by IntelliJ) (see # Run the app)
    - verify that some logs are printed on the Main window in IntelliJ
    
- On Preferences/Code Style/Scala 
  - choose "Scalafmt" as Formatter
  - check the "Reformat on file save" option

### Run the app and launching tests

```bash
sbt             # to enter the sbt console
generateCode    # to be aligned with recent changes to the API
compile
run
```

### API UI from browser
- When the app is running use the following link to access the API [swagger](http://127.0.0.1:8093/datamesh.specificprovisioner/0.0/swagger/docs/index.html)