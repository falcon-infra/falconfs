#!/bin/bash
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#
#                                                                                   #
# This script copies all shared library dependencies from a binary source file to   #
# a desired target directory. The directory structure of the libraries will NoT to  #
# be preserved. The binary file itself will also be copied to the target directory. #
#                                                                                   #
# OPTION [-b]: Full path to the binary whose dependencies shall be copied.          # 
# OPTION [-t]: Full path to the target directory for the dependencies.              #
#                                                                                   #
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%#

# Parsing command-line arguments with the getopts shell builtin
while getopts :b:t: option; do
    case $option in
    b) ARGUMENT_BINARY="$OPTARG" ;;
    t) ARGUMENT_TARGET="$OPTARG" ;;
    esac
done

echo ${ARGUMENT_BINARY}
echo ${ARGUMENT_TARGET}

# Checking if all required command-line arguments are provided
[ -z "${ARGUMENT_BINARY}" ] && echo "$0: Missing argument: [-b binary]" >&2
[ -z "${ARGUMENT_TARGET}" ] && echo "$0: Missing argument: [-t target]" >&2

# Abort execution if required command-line argument is missing
[ -z "${ARGUMENT_BINARY}" ] || [ -z "${ARGUMENT_TARGET}" ] && exit 1

# Checking if binary or target path does not exists and abort
[ ! -f "${ARGUMENT_BINARY}" ] && echo "$0: Binary path does not exists." >&2 && exit 1
[ ! -d "${ARGUMENT_TARGET}" ] && echo "$0: Target path does not exists." >&2 && exit 1

# Copy binary file to the target directory (without preserving path)
#binary_name=$(basename "${ARGUMENT_BINARY}")
#cp --verbose "${ARGUMENT_BINARY}" "${ARGUMENT_TARGET}/${binary_name}"

# Copy each library to the target directory (without preserving path)
for library in $(ldd "${ARGUMENT_BINARY}" | awk '/=>/ {print $3}'); do
    if [ -f "${library}" ]; then
        library_name=$(basename "${library}")
        cp --verbose "${library}" "${ARGUMENT_TARGET}/${library_name}"
    fi
done
