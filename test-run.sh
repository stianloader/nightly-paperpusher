mvn -DaltDeploymentRepository=local::http://localhost:8080/maven/ deploy
curl http://localhost:8080/maven/commit
curl -s -o /dev/null http://localhost:8080/javadocs/
curl -s -o /dev/null http://localhost:8080/javadocs/org.stianloader/nightly-paperpusher/[0,999\)/index.html
curl http://localhost:8080/killdaemon
