mvn clean install -DskipTests

mvn dependency:copy-dependencies -DincludeScope=provided -DoutputDirectory=target