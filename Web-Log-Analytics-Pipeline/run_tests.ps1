$jars = (Get-ChildItem lib/*.jar | Select-Object -ExpandProperty FullName) -join ';'
$cp = "target/classes;target/test-classes;src/test/resources;$jars"
java -javaagent:lib/jacocoagent.jar=destfile=target/jacoco.exec -cp "$cp" org.junit.platform.console.ConsoleLauncher execute --select-package edu.bigdata.clickstream --reports-dir target/surefire-reports
