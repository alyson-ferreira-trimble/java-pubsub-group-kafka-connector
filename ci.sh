#!/bin/bash

function main() {
    local target="$1"

    load_dotenv

    case "$target" in
        "release")
            release
            ;;
        "snapshot")
            snapshot
            ;;
        "build")
            mvn clean install
            ;;
        "test")
            mvn_test
            ;;
        *)
            usage
            ;;
    esac
}

function release() {
    if ! validate_environment_for_deploy; then
        exit 1
    fi

    mvn_test
    mvn_deploy
}

function snapshot() {
    if ! validate_environment_for_deploy; then
        exit 1
    fi

    mvn_test
    backup_pom
    set_snapshot_version
    mvn_deploy
    reset
}

function usage() {
    echo "Usage: $0 <release|snapshot|build|test>"
    exit 1
}

function validate_environment_for_deploy() {
    outcome=0
    for var in \
        ARTIFACTORY_CONTEXT_URL \
        ARTIFACTORY_REPO_KEY \
        ARTIFACTORY_SNAPSHOT_REPO_KEY \
        ARTIFACTORY_USERNAME \
        ARTIFACTORY_PASSWORD; do
        
        if [ -z "${!var}" ]; then
            echo "${var} must be set"
            outcome=1
        fi
    done

    return $outcome
}

function load_dotenv() {
    # Doesn't work with spaces in values and special characters for bash
    local dotenv_file="${1:-ci.env}"
    if [ -f "$dotenv_file" ]; then
        export $(grep -v '^#' "$dotenv_file" | xargs)
    fi
}

function mvn_test() {
    mvn clean test
}

function mvn_deploy() {
    # -Dclirr.skip https://www.mojohaus.org/clirr-maven-plugin/check-mojo.html#skip
    # -DskipNexusStagingDeployMojo See mvn nexus-staging:help -Ddetail=true -Dgoal=deploy
    mvn clean deploy -DskipTests -DskipNexusStagingDeployMojo -Dclirr.skip -P release
}

function set_snapshot_version() {
    local version
    version="$(xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" pom.xml)"
    snapshot_version="${version%-SNAPSHOT}-SNAPSHOT"
    echo "snapshot_version: ${snapshot_version}"
    mvn versions:set -DnewVersion="${snapshot_version}" -DgenerateBackupPoms=false
}

function backup_pom() {
    if [ -f pom.xml.original ]; then
        echo "pom.xml.original already exists"
        exit 1
    fi

    cp pom.xml pom.xml.original
}

function reset() {
    if [ -f pom.xml.original ]; then
        mv pom.xml.original pom.xml
    fi
}

trap 'reset' ERR EXIT INT TERM
set -e
main "$@"