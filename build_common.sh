#!/usr/bin/bash
cd /c/Projects/hudi-GG/hudi-common
mvn clean package -D"checkstyle.skip"=true -DskipTests -DskipITs -Drat.skip=true
cd /c/Projects/hudi-GG