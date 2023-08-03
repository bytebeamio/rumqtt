# shellcheck shell=sh

# ================
# GLOBALS
# ================
# Return value
RETVAL=

# ================
# PARSE ARGUMENTS
# ================
# Assert argument has a value
# @param $1 Argument name
# @param $2 Argument value
parse_args_assert_value() {
    [ -n "$2" ] || FATAL "Argument '$1' requires a non-empty value"
}
# Parse command line arguments
# @param $@ Arguments
parse_args_commons() {
    # Number of shift
    _shifts=1

    # Parse
    case $1 in
        --disable-color)
            # Disable color
            LOG_COLOR_ENABLE=false
            ;;
        --log-level)
            # Log level
            parse_args_assert_value "$@"

            case $2 in
                fatal) LOG_LEVEL=$LOG_LEVEL_FATAL ;;
                error) LOG_LEVEL=$LOG_LEVEL_ERROR ;;
                warn) LOG_LEVEL=$LOG_LEVEL_WARN ;;
                info) LOG_LEVEL=$LOG_LEVEL_INFO ;;
                debug) LOG_LEVEL=$LOG_LEVEL_DEBUG ;;
                *) FATAL "Value '$2' of argument '$1' is invalid" ;;
            esac
            _shifts=2
            ;;
        -*)
            # Unknown argument
            WARN "Unknown argument '$1' is ignored"
            ;;
        *)
            # No argument
            WARN "Skipping argument '$1'"
            ;;
    esac

    # Return
    # shellcheck disable=SC2034
    RETVAL=$_shifts
}

# ================
# LOGGER
# ================
# Fatal log level. Cause exit failure
LOG_LEVEL_FATAL=100
# Error log level
LOG_LEVEL_ERROR=200
# Warning log level
LOG_LEVEL_WARN=300
# Informational log level
LOG_LEVEL_INFO=500
# Debug log level
LOG_LEVEL_DEBUG=600
# Log level
LOG_LEVEL=$LOG_LEVEL_INFO
# Log color flag
LOG_COLOR_ENABLE=true

# Convert log level to equivalent name
# @param $1 Log level
log_level_name() {
    _log_level=${1:-LOG_LEVEL}
    _log_level_name=

    case $_log_level in
        "$LOG_LEVEL_FATAL") _log_level_name=fatal ;;
        "$LOG_LEVEL_ERROR") _log_level_name=error ;;
        "$LOG_LEVEL_WARN") _log_level_name=warn ;;
        "$LOG_LEVEL_INFO") _log_level_name=info ;;
        "$LOG_LEVEL_DEBUG") _log_level_name=debug ;;
        *) FATAL "Unknown log level '$_log_level'" ;;
    esac

    printf '%s\n' "$_log_level_name"
}

# Check if log level is enabled
# @param $1 Log level
log_is_enabled() {
    [ "$1" -le "$LOG_LEVEL" ]
}

# Print log message
# @param $1 Log level
# @param $2 Message
_log_print_message() {
    _log_level=${1:-LOG_LEVEL_FATAL}
    shift
    _log_level_name=
    _log_message=${*:-}
    _log_prefix=
    _log_suffix="\033[0m"

    # Check log level
    log_is_enabled "$_log_level" || return 0

    case $_log_level in
        "$LOG_LEVEL_FATAL")
            _log_level_name=FATAL
            _log_prefix="\033[41;37m"
            ;;
        "$LOG_LEVEL_ERROR")
            _log_level_name=ERROR
            _log_prefix="\033[1;31m"
            ;;
        "$LOG_LEVEL_WARN")
            _log_level_name=WARN
            _log_prefix="\033[1;33m"
            ;;
        "$LOG_LEVEL_INFO")
            _log_level_name=INFO
            _log_prefix="\033[37m"
            ;;
        "$LOG_LEVEL_DEBUG")
            _log_level_name=DEBUG
            _log_prefix="\033[1;34m"
            ;;
    esac

    # Check color flag
    if [ "$LOG_COLOR_ENABLE" = false ]; then
        _log_prefix=
        _log_suffix=
    fi

    # Log
    printf '%b[%-5s] %b%b\n' "$_log_prefix" "$_log_level_name" "$_log_message" "$_log_suffix"
}

# Fatal log message
# @param $1 Message
FATAL() {
    _log_print_message "$LOG_LEVEL_FATAL" "$1" >&2
    exit 1
}
# Error log message
# @param $1 Message
ERROR() { _log_print_message "$LOG_LEVEL_ERROR" "$1" >&2; }
# Warning log message
# @param $1 Message
WARN() { _log_print_message "$LOG_LEVEL_WARN" "$1" >&2; }
# Informational log message
# @param $1 Message
INFO() { _log_print_message "$LOG_LEVEL_INFO" "$1"; }
# Debug log message
# @param $1 Message
DEBUG() { _log_print_message "$LOG_LEVEL_DEBUG" "$1"; }

# ================
# ASSERT
# ================
# Assert command is installed
# @param $1 Command name
assert_cmd() {
    check_cmd "$1" || FATAL "Command '$1' not found"
    DEBUG "Command '$1' found at '$(command -v "$1")'"
}

# ================
# FUNCTIONS
# ================
# Check command is installed
# @param $1 Command name
check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

# ================
# CONFIGURATION
# ================
# Log level
LOG_LEVEL=$LOG_LEVEL_INFO
# Log color flag
LOG_COLOR_ENABLE=true
