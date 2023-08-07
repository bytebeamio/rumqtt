#!/usr/bin/env sh

# Fail on error
set -o errexit
# Disable wildcard character expansion
set -o noglob
# Disable uninitialized variable usage
set -o nounset
# Disable error masking in pipe
# shellcheck disable=SC3040
if (set -o pipefail 2> /dev/null); then
    set -o pipefail
fi

# Current directory
# shellcheck disable=SC1007
DIRNAME=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

# Load commons
# shellcheck source=SCRIPTDIR/../../scripts/__commons.sh
. "$DIRNAME/../../scripts/__commons.sh"

# ================
# CONFIGURATION
# ================
# Rust version
RUMQTTD_RUST_VERSION=$(
    _cargo_toml=""
    _cargo_toml_root="$DIRNAME/../../Cargo.toml"
    _cargo_toml_rumqttd="$DIRNAME/../Cargo.toml"
    if grep -m 1 -q -P '^[[:blank:]]*rust-version\.workspace[[:blank:]]*=[[:blank:]]*true[[:blank:]]*$' "$_cargo_toml_rumqttd"; then
        _cargo_toml=$_cargo_toml_root
    else
        _cargo_toml=$_cargo_toml_rumqttd
    fi

    grep -m 1 'rust-version' "$_cargo_toml" | grep -o -P '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)'
)
# Docker platform
RUMQTTD_DOCKER_PLATFORM="linux/amd64,linux/arm64"
# Dockerfile
RUMQTTD_DOCKER_DOCKERFILE="$DIRNAME/../Dockerfile"
# Docker image repository
RUMQTTD_DOCKER_IMAGE_REPOSITORY="registry-1.docker.io"
# Docker image name
RUMQTTD_DOCKER_IMAGE_NAME="bytebeamio/rumqttd"
# Docker image version
RUMQTTD_DOCKER_IMAGE_VERSION=$(grep -m 1 'version' "$DIRNAME/../Cargo.toml" | grep -o -P '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)')
# Docker context
RUMQTTD_DOCKER_CONTEXT="$DIRNAME/../.."

# ================
# FUNCTIONS
# ================
# Show help message
show_help() {
    cat << EOF
Usage: $(basename "$0") [--context <DIR>] [--disable-color] [--dockerfile <FILE>]
                        [--help] [--log-level <LEVEL>] [--name <NAME>]
                        [--platform <PLATFORM>] [--repository <URL>] [--rust-version <VERSION>]
                        [--version <VERSION>]

rumqttd Docker image builder.

Options:
  --context <DIR>           Docker context directory
                            Default: $RUMQTTD_DOCKER_CONTEXT

  --disable-color           Disable color

  --dockerfile <FILE>       Dockerfile file
                            Default: $RUMQTTD_DOCKER_DOCKERFILE

  --help                    Show this help message and exit

  --log-level <LEVEL>       Logger level
                            Default: $(log_level_name "$LOG_LEVEL")
                            Values:
                              fatal    Fatal level
                              error    Error level
                              warn     Warning level
                              info     Informational level
                              debug    Debug level

  --name <NAME>             Docker image name
                            Default: $RUMQTTD_DOCKER_IMAGE_NAME

  --platform <PLATFORM>     Docker platform
                            Default: $RUMQTTD_DOCKER_PLATFORM

  --repository <URL>        Docker image repository
                            Default: $RUMQTTD_DOCKER_IMAGE_REPOSITORY

  --rust-version <VERSION>  Rust version
                            Default: $RUMQTTD_RUST_VERSION

  --version <VERSION>       Docker image version
                            Default: $RUMQTTD_DOCKER_IMAGE_VERSION
EOF
}

# Parse command line arguments
# @param $@ Arguments
parse_args() {
    while [ $# -gt 0 ]; do
        # Number of shift
        _shifts=1

        case $1 in
            --context)
                # Docker context
                parse_args_assert_value "$@"

                RUMQTTD_DOCKER_CONTEXT=$2
                _shifts=2
                ;;
            --dockerfile)
                # Dockerfile
                parse_args_assert_value "$@"

                RUMQTTD_DOCKER_DOCKERFILE=$2
                _shifts=2
                ;;
            --help)
                # Display help message and exit
                show_help
                exit 0
                ;;
            --name)
                # Docker image name
                parse_args_assert_value "$@"

                RUMQTTD_DOCKER_IMAGE_NAME=$2
                _shifts=2
                ;;
            --platform)
                # Docker platform
                parse_args_assert_value "$@"

                RUMQTTD_DOCKER_PLATFORM=$2
                _shifts=2
                ;;
            --repository)
                # Docker repository
                parse_args_assert_value "$@"

                RUMQTTD_DOCKER_IMAGE_REPOSITORY=$2
                _shifts=2
                ;;
            --rust-version)
                # Rust version
                parse_args_assert_value "$@"

                RUMQTTD_RUST_VERSION=$2
                _shifts=2
                ;;
            --version)
                # Docker image version
                parse_args_assert_value "$@"

                RUMQTTD_DOCKER_IMAGE_VERSION=$2
                _shifts=2
                ;;
            *)
                # Commons
                parse_args_commons "$@"
                _shifts=$RETVAL
                ;;
        esac

        # Shift arguments
        while [ "$_shifts" -gt 0 ]; do
            shift
            _shifts=$((_shifts = _shifts - 1))
        done
    done
}

# Verify system
verify_system() {
    DEBUG "Verifying system"

    assert_cmd docker

    _docker_tag="$RUMQTTD_DOCKER_IMAGE_REPOSITORY/$RUMQTTD_DOCKER_IMAGE_NAME:$RUMQTTD_DOCKER_IMAGE_VERSION"

    [ -f "$RUMQTTD_DOCKER_DOCKERFILE" ] || FATAL "Dockerfile '$RUMQTTD_DOCKER_DOCKERFILE' does not exists"
    [ -d "$RUMQTTD_DOCKER_CONTEXT" ] || FATAL "Docker context '$RUMQTTD_DOCKER_CONTEXT' does not exists"
    printf "%s\n" "$RUMQTTD_RUST_VERSION" | grep -q -P '(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)' || FATAL "Rust version '$RUMQTTD_RUST_VERSION' is not valid"
    printf "%s\n" "$RUMQTTD_DOCKER_PLATFORM" | grep -q -P '^linux\/(amd64|arm64)(,linux\/(amd64|arm64))*$' || FATAL "Docker platform '$RUMQTTD_DOCKER_PLATFORM' is not valid"
    printf "%s\n" "$_docker_tag" | grep -q -P '^(?<Name>(?<=^)(?:(?<Domain>(?:(?:localhost|[\w-]+(?:\.[\w-]+)+)(?::\d+)?)|[\w]+:\d+)\/)?\/?(?<Namespace>(?:(?:[a-z0-9]+(?:(?:[._]|__|[-]*)[a-z0-9]+)*)\/)*)(?<Repo>[a-z0-9-]+))[:@]?(?<Reference>(?<=:)(?<Tag>[\w][\w.-]{0,127})|(?<=@)(?<Digest>[A-Za-z][A-Za-z0-9]*(?:[-_+.][A-Za-z][A-Za-z0-9]*)*[:][0-9A-Fa-f]{32,}))?' || FATAL "Docker tag '$_docker_tag' is not valid"

    DEBUG "Successfully verified system"
}

# Build Docker image
build_docker_image() {
    _tag_version="$RUMQTTD_DOCKER_IMAGE_REPOSITORY/$RUMQTTD_DOCKER_IMAGE_NAME:$RUMQTTD_DOCKER_IMAGE_VERSION"
    _tag_latest="$RUMQTTD_DOCKER_IMAGE_REPOSITORY/$RUMQTTD_DOCKER_IMAGE_NAME:latest"

    INFO "Building Docker image"

    docker buildx build \
        --platform "$RUMQTTD_DOCKER_PLATFORM" \
        --file "$RUMQTTD_DOCKER_DOCKERFILE" \
        --build-arg "RUST_VERSION=$RUMQTTD_RUST_VERSION" \
        --tag "$_tag_version" \
        --tag "$_tag_latest" \
        --push \
        "$RUMQTTD_DOCKER_CONTEXT"

    DEBUG "Successfully built Docker image"
}

# ================
# MAIN
# ================
{
    parse_args "$@"
    verify_system
    build_docker_image
}
