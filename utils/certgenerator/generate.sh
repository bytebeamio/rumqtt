#! /bin/bash

# references
#-------------
# https://jamielinux.com/docs/openssl-certificate-authority/create-the-root-pair.html
# https://www.geocerts.com/ssl-tools
set -e


HOME=$PWD
OUT=$PWD/out
META=$PWD/meta
META_ROOT=$PWD/meta/root
META_INTERMEDIATE=$PWD/meta/intermediate
COLOR='\033[0;34m'
NC='\033[0m' # No Color

help() {
    echo "Example usage: $0 --client {client_id}"
    echo "Example usage: $0 --server {domain_name}"
    echo
    echo "   --root           | -r                     Delete all the existing certificates and generate new root & intermediate certificates"
    echo "   --inter          | -i                     Generate new intermediate certificates"
    echo "   --server         | -s                     Generate new server certificates using existing intermediate tlsfiles"
    echo "   --client         | -c                     Generate new client certificates using existing intermediate tlsfiles"
    echo "   --check-root     | -cr                    Check root ca certificate"
    echo "   --check-inter    | -ci                    Check intermedaite ca certificate"
    echo "   --check-server   | -cs                    Check server certificate"
    echo "   --check-client   | -cc                    Check client certificate"
    echo "   --verify-inter   | -vi                    Verify inermediate ca certificate"
    echo "   --verify-server  | -vs                    Verify server certificate"
    echo "   --verify-client  | -vc                    Verify client certificate"
    echo "   --test                                    Instructions for starting test server & client"
    
    echo
    # echo some stuff here for the -a or --add-options 
    exit 1
}

info() {
    echo -e "${COLOR}************************************************************${NC}"
    echo -e "${COLOR}$1${NC}"
    echo -e "${COLOR}$2${NC}"
    echo -e "${COLOR}************************************************************${NC}"
}

gen_root_key_cert() {
    mkdir -p $OUT/root
    cd $OUT/root
    mkdir tlsfiles crl newcerts private
    chmod 700 private
    touch index.txt
    echo 1000 > serial

    openssl genrsa -aes256 -out private/ca.key.pem 4096
    chmod 400 private/ca.key.pem

    # 'req -new -x509' creates new self signed certificate (root ca here)
    openssl req                      \
            -config $META/rootca.cnf \
            -key private/ca.key.pem  \
            -new -x509 -days 7300    \
            -out tlsfiles/ca.cert.pem
    
    chmod 444 tlsfiles/ca.cert.pem
    cd $HOME
}

gen_inter_key_cert() {
    mkdir -p $OUT/intermediate
    cd $OUT/intermediate
    mkdir tlsfiles crl csr newcerts private
    chmod 700 private
    touch index.txt
    echo 1000 > serial
    echo 1000 > crlnumber

    info "generating intermediate key. set key password"
    openssl genrsa -aes256 -out private/intermediate.key.pem 4096
    chmod 400 private/intermediate.key.pem

    info "creating signing request for intermediate cert generation. \
          \nuse defaults. requires intermediate key password"
    openssl req                                 \
            -config $META/interca.cnf           \
            -new -sha256                        \
            -key private/intermediate.key.pem   \
            -out csr/intermediate.csr.pem

    info "signing csr to generate intermediate ca certificate. requires root key password"

    openssl ca                                          \
            -config $META/rootca.cnf                    \
            -extensions v3_intermediate_ca -days 3650   \
            -notext -md sha256                          \
            -in csr/intermediate.csr.pem                \
            -out tlsfiles/intermediate.cert.pem

    chmod 444 tlsfiles/intermediate.cert.pem

    info "creating certificate chain file"
    cat tlsfiles/intermediate.cert.pem ../root/tlsfiles/ca.cert.pem > tlsfiles/ca-chain.cert.pem
    chmod 444 tlsfiles/ca-chain.cert.pem
    cd $HOME
}

gen_server_cert_key() {
    rm -rf $OUT/server
    mkdir -p $OUT/server
    cd $OUT/server
    mkdir tlsfiles crl csr private
    chmod 700 private

    openssl ca -config $META/interca.cnf -revoke tlsfiles/server.cert.pem || true

    info "generating server key"
    openssl genrsa -out private/server.key.pem 2048
    chmod 400 private/server.key.pem

    info "creating signing request for server cert generation", \
         "NOTE: for server, common name should be fully qualified domain name"

    # NOTE: SAN is important for strict tls stacks like rustls. SAN in csr
    # doesn't guarantee SAN in the certificate. Intermediate ca conf should
    # be configured to allow SAN extension. This is bypassed with `copyall`
    # for now but that is not safe. Clean this up later
    # we can use the tools here to verify -> https://www.geocerts.com/ssl-tools
    openssl req                                             \
            -new -sha256                                    \
            -key private/server.key.pem                     \
            -out csr/server.csr.pem                         \
            -reqexts SAN                                    \
            -config <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=DNS:$1")) \
            -subj "/C=IN/ST=Karnataka/L=Victoria Layout/O=Blah Blah Pvt Ltd/OU=Software Team/CN=$1"



    info "signing csr with intermediate ca to generate server certificate"
    openssl ca                                  \
            -config $META/interca.cnf           \
            -extensions server_cert -days 375   \
            -notext -md sha256                  \
            -in csr/server.csr.pem              \
            -out tlsfiles/server.cert.pem


    chmod 444 tlsfiles/server.cert.pem
    cd $HOME
    instructions
}

gen_client_cert_key() {
    mkdir -p $OUT/client || true
    cd $OUT/client
    mkdir tlsfiles crl csr private || true
    chmod 700 private

    info "generating client key"
    openssl genrsa -out private/$1.key.pem 2048
    chmod 400 private/$1.key.pem

    info "creating signing request for client cert generation", \
         "NOTE: for client, common name should be a unique name"

    openssl req                                 \
            -key private/$1.key.pem             \
            -new -sha256 -out csr/$1.csr.pem    \
            -subj "/C=IN/ST=Karnataka/L=Victoria Layout/O=Blah Blah Pvt Ltd/OU=Software Team/CN=$1"

    info "generating client certificate signed with intermediate ca"
    openssl ca                                          \
            -config $META/interca.cnf                   \
            -extensions usr_cert -days 375              \
            -notext -md sha256                          \
            -in csr/$1.csr.pem                          \
            -out tlsfiles/$1.cert.pem

    chmod 444 tlsfiles/$1.cert.pem
    cd $HOME
    instructions
}

validate_server_args () {
    if [ -z "$1" ]; then
        echo -e "\nPlease call '$0 --server {domain_name}' to run this command!\n"
        exit 1
    fi
}

validate_client_args () {
    if [ -z "$1" ]; then
        echo -e "\nThis command requires client id as \n"
        exit 1
    fi
}

instructions () {
    info "use server/tlsfiles/server.cert.pem, server/private/server.key.pem, intermediate/tlsfiles/ca-chain.cert.pem on server side", \
         "use client/tlsfiles/client.cert.pem, client/private/client.key.pem, intermediate/tlsfiles/ca-chain.cert.pem on client side"
}


check_root () {
    cd $OUT/root
    openssl x509 -noout -text -in tlsfiles/ca.cert.pem
    cd $HOME
}

check_inter() {
    cd $OUT/intermediate
    openssl x509 -noout -text -in tlsfiles/intermediate.cert.pem
    cd $HOME
}

check_server() {
    cd $OUT/server
    info "checking server csr"
    openssl req -in csr/server.csr.pem -noout -text
    info "checking server certificate"
    openssl x509 -noout -text -in tlsfiles/server.cert.pem
    cd $HOME
}

check_client() {
    cd $OUT/client

    openssl x509 -noout -text -in tlsfiles/$1.cert.pem
    cd $HOME
}

verify_inter() {
    cd $OUT
    openssl verify -CAfile root/tlsfiles/ca.cert.pem intermediate/tlsfiles/intermediate.cert.pem
    cd $HOME
}

verify_server() {
    cd $OUT
    openssl verify -CAfile intermediate/tlsfiles/ca-chain.cert.pem server/tlsfiles/server.cert.pem
    cd $HOME
}

verify_client() {
    cd $OUT
    openssl verify -CAfile intermediate/tlsfiles/ca-chain.cert.pem client/tlsfiles/$1.cert.pem
    cd $HOME
}

test() {
    # -verify in server makes sure that client sends its certificate
    info "
    copy below command in a seperate terminal
    openssl s_server -accept 12345 -verify \\
                                   -tls1_2 \\
                                   -cert out/server/tlsfiles/server.cert.pem \\
                                   -key out/server/private/server.key.pem \\
                                   -CAfile out/intermediate/tlsfiles/ca-chain.cert.pem

    NOTE: return status should be 0 and you'll see send text on the server side
    "
    read -p "Press a key after starting the server" choice
    openssl s_client -connect 0.0.0.0:12345 -tls1_2 -cert out/client/tlsfiles/$1.cert.pem -key out/client/private/$1.key.pem -CAfile out/intermediate/tlsfiles/ca-chain.cert.pem
}

clean_root ()
{
    rm -rf $OUT
}

clean_inter ()
{
    rm -rf $OUT/intermediate
    rm -rf $OUT/server
    rm -rf $OUT/client
}



#------------------- start processing input arguments ------------------------

if [ $# -eq 0 ]; then
    help
fi

case $1 in
    # generate new client using existing intermediate certificates
    -c|--client)
        validate_client_args $2
        gen_client_cert_key $2
        exit
        ;;

    -s|--server)
        validate_server_args $2
        gen_server_cert_key $2
        exit
        ;;
    # Clean and generate new root certificate authority
    -r|--root)
        read -p "Are you sure you want to remove old root & intermediate tlsfiles & start from scratch (y/n)?" choice
        case "$choice" in
        y|Y )
            echo "yes"
            clean_root
            gen_root_key_cert
            exit
            ;;
        n|N )
            echo "exiting"
            exit
            ;;
        * )
            echo "invalid"
            ;;
        esac
        ;;
    -i|--inter)
        read -p "Are you sure you want to remove old intermediate ca files and create brand new intermediate ca?" choice
        case "$choice" in
        y|Y )
            echo "yes"
            clean_inter
            gen_inter_key_cert
            exit
            ;;
        n|N )
            echo "exiting"
            exit
            ;;
        * )
            echo "invalid"
            ;;
        esac
        ;;
    -cr|--check-root)
        check_root
        exit
        ;;

    -ci|--check-inter)
        check_inter
        exit
        ;;

    -cs|--check-server)
        check_server
        exit
        ;;

    -cc|--check-client)
        check_client
        exit
        ;;

    -vi|--verify-inter)
        verify_inter
        exit
        ;;

    -vs|--verify-server)
        verify_server
        exit
        ;;

    -vc|--verify-client)
        validate_client_args $2
        verify_client $2
        exit
        ;;
    --test)
        validate_client_args $2
        test $2
        exit
        ;;

    # show help
    * )
        help
        exit
        ;;
esac
