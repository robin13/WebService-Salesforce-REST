package WebService::Salesforce::REST;
# ABSTRACT: Interface to Salesforce REST API
use Moose;
use MooseX::Params::Validate;
use MooseX::WithCache;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Headers;
use HTTP::Message;
use JSON;
use Class::Date qw/gmdate/;
use POSIX; #strftime
use YAML qw/Dump LoadFile DumpFile/;
use Encode;
use URI::Encode qw/uri_encode/;

our $VERSION = 0.001;

=head1 NAME

WebService::Salesforce::REST

=head1 DESCRIPTION

Interaction with Salesforce REST API

This module uses MooseX::Log::Log4perl for logging - be sure to initialize!

=cut


=head1 ATTRIBUTES

=over 4

=item cache

Optional.

Provided by MooseX::WithX - optionally pass a Cache::FileCache object to cache and avoid unnecessary requests

=cut

with "MooseX::Log::Log4perl";

# Unfortunately it is necessary to define the cache type to be expected here with 'backend'
# TODO a way to be more generic with cache backend would be better
with 'MooseX::WithCache' => {
    backend => 'Cache::FileCache',
};

=item username

=cut
has 'username' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    writer      => '_set_username',
    );

=item password

=cut
has 'password' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    writer      => '_set_password',
    );

=item security_token

=cut
has 'security_token' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    writer      => '_set_security_token',
    );

=item client_id

=cut
has 'client_id' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    writer      => '_set_client_id',
    );

=item client_secret

=cut
has 'client_secret' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    writer      => '_set_client_secret',
    );

=item access_token

=cut
has 'access_token' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    writer      => '_set_access_token',
    );
=item is_sandbox

=cut
has 'is_sandbox' => (
    is		=> 'ro',
    isa		=> 'Bool',
    required	=> 1,
    default     => 0,
    writer      => '_set_is_sandbox',
    );

=item instance_url

=cut
has 'instance_url' => (
    is		=> 'ro',
    isa		=> 'Str',
    required	=> 0,
    writer      => '_set_instance_url',
    );

=item api_version

=cut
has 'api_version' => (
    is		=> 'ro',
    isa		=> 'Str',
    required	=> 1,
    default     => 'v36.0',
    );

=item credentials_file

=cut
has 'credentials_file' => (
    is          => 'ro',
    isa         => 'Str',
    required    => 0,
    trigger     => \&_load_credentials,
    );

=item timeout

Timeout in seconds.  Optional.  Default: 10 
Will only be in effect if you allow the useragent to be built in this module.

=cut
has 'timeout' => (
    is          => 'ro',
    isa         => 'Int',
    required    => 1,
    default     => 10,
    );

=item default_backoff

Optional.  Default: 10
Time in seconds to back off before retrying request.
If a 429 response is given and the Retry-Time header is provided by the api this will be overridden.

=cut
has 'default_backoff' => (
    is          => 'ro',
    isa         => 'Int',
    required    => 1,
    default     => 10,
    );

=item default_page_size

Optional. Default: 100

=cut
has 'default_page_size' => (
    is          => 'rw',
    isa         => 'Int',
    required    => 1,
    default     => 100,
    );

=item retry_on_status

Optional. Default: [ 429, 500, 502, 503, 504 ]
Which http response codes should we retry on?

=cut
has 'retry_on_status' => (
    is          => 'ro',
    isa         => 'ArrayRef',
    required    => 1,
    default     => sub{ [ 429, 500, 502, 503, 504 ] },
    );

=item max_tries

Optional.  Default: undef

Limit maximum number of times a query should be attempted before failing.  If undefined then unlimited retries

=cut
has 'max_tries' => (
    is          => 'ro',
    isa         => 'Int',
    );


=item user_agent

Optional.  A new LWP::UserAgent will be created for you if you don't already have one you'd like to reuse.

=cut

has 'user_agent' => (
    is		=> 'ro',
    isa		=> 'LWP::UserAgent',
    required	=> 1,
    lazy	=> 1,
    builder	=> '_build_user_agent',

    );

=item loglevel

Optionally override the global loglevel for this module

=cut

has 'loglevel' => (
    is		=> 'rw',
    isa		=> 'Str',
    trigger     => \&_set_loglevel,
    );


has '_headers' => (
    is          => 'ro',
    isa         => 'HTTP::Headers',
    writer      => '_set_headers',
    clearer     => '_clear_headers',
    );

sub _set_loglevel {
    my( $self, $level ) = @_;
    $self->log->warn( "Setting new loglevel: $level" );
    $self->log->level( $level );
}

sub _load_credentials {
    my( $self, $credentials_file ) = @_;
    $self->log->debug( "Trying to read credentials from file: " . $credentials_file );

    if( not -f $self->credentials_file ){
        $self->log->logdie( "Not a file: " . $credentials_file );
    }
    my $credentials = LoadFile ( $credentials_file );
    foreach( qw/username password security_token client_id client_secret access_token is_sandbox instance_url/ ){
        my $method = '_set_' . $_;
        $self->$method( $credentials->{$_} ) if( $credentials->{$_} );
    }
}

sub _save_current_access_token_to_credentials_file {
    my( $self, $credentials_file ) = @_;
    $self->log->debug( "Saving credentials to file: " . $credentials_file );
    
    my $credentials = {};
    if( -f $self->credentials_file ){
        $credentials = LoadFile ( $credentials_file );
    }
    $credentials->{access_token} = $self->access_token;
    $credentials->{instance_url} = $self->instance_url;

    DumpFile ( $credentials_file, $credentials );
}

sub _build_user_agent {
    my $self = shift;
    $self->log->debug( "Building useragent" );
    my $ua = LWP::UserAgent->new(
	keep_alive	=> 1,
        timeout         => $self->timeout,
    );
    return $ua;
}

=back

=head1 METHODS

=over 4

=item refresh_access_token

Will return a valid access token.

=cut

sub refresh_access_token {
    my ( $self, %params ) = validated_hash(
        \@_,
        username                => { isa    => 'Str', optional => 1 },
        password                => { isa    => 'Str', optional => 1 },
        security_token          => { isa    => 'Str', optional => 1 },
        client_id               => { isa    => 'Str', optional => 1 },
        client_secret           => { isa    => 'Str', optional => 1 },
	);
    
    my @required_for_login = qw/username password security_token client_id client_secret/;
    
    # If not passed, see if the object has the necessary parameters
    foreach( @required_for_login ){
        $params{$_}  ||= $self->$_ if $self->$_;
        if( not $params{$_} ){
            $self->log->logdie( "Cannot log in without parameter: $_" );
        }
    }
    
    $self->log->debug( "Requesting access_token for: $params{username}" );
    my $h = HTTP::Headers->new();
    $h->header( "Content-Type" => "application/x-www-form-urlencoded" );
    $h->header( 'Accept'	=> "application/json" );
    my $data = $self->_request_from_api(
        headers     => $h,
        uri         => 'https://' . ( $self->is_sandbox ? 'test' : 'login' ) . '.salesforce.com',
        path        => '/services/oauth2/token',
        body       => sprintf( 'grant_type=password&username=%s&password=%s%s&client_id=%s&client_secret=%s',
                                uri_encode( $params{username} ),
                                uri_encode( $params{password} ), uri_encode( $params{security_token} ),
                                uri_encode( $params{client_id} ),
                                uri_encode( $params{client_secret} )
        ),
        );

    $self->log->trace( "Response from getting access_token:\n" . Dump( $data ) ) if $self->log->is_trace();
    $self->log->debug( "Got new access_token: $data->{access_token}" );
    
    $self->_set_access_token( $data->{access_token} );
    $self->_set_instance_url( $data->{instance_url} );
    $self->_clear_headers;
    
    if( $self->credentials_file ){
        $self->_save_current_access_token_to_credentials_file( $self->credentials_file );
    }
    return $data->{access_token};
}

=item headers

Returns a HTTP::Headers object with the Authorization header set with a valid access token

=cut
sub headers {
    my $self = shift;
    if( not $self->_headers ){
        if( not $self->access_token ){
            $self->refresh_access_token;
        }
        my $h = HTTP::Headers->new();
        $h->header( 'Content-Type'      => "application/json" );
        $h->header( 'Accept-Encoding'   => HTTP::Message::decodable );
        $h->header( 'Accept'	        => "application/json" );
        $h->header( 'Authorization'     => "Bearer " . $self->access_token );
        $self->_set_headers( $h );
    }
    return $self->_headers;
}

=back

=head1 API METHODS

This is a module in development - only a subset of all of the API endpoints have been implemented yet.


=over 4

=item query

execute a query

=cut

sub query {
    my ( $self, %params ) = validated_hash(
        \@_,
        query	    => { isa    => 'Str', optional => 1 },
        options	    => { isa    => 'Str', optional => 1 },
	);
    $params{path}   = '/services/data/' . $self->api_version . '/query/';
    $params{method} = 'GET';
    $params{options} .= ( $params{options} ? '&' : '' ) . 'q=' . uri_encode( $params{query} );
    delete( $params{query} );

    return $self->_request_from_api( %params );
}



sub _request_from_api {
    my ( $self, %params ) = validated_hash(
        \@_,
        method	=> { isa => 'Str', optional => 1, default => 'POST' },
	path	=> { isa => 'Str', optional => 1 },
        uri     => { isa => 'Str', optional => 1 },
        body    => { isa => 'Str', optional => 1 },
        headers => { isa => 'HTTP::Headers', optional => 1 },
        options => { isa => 'Str', optional => 1 },
    );
    $params{headers} ||= $self->headers;

    my $url = $params{uri} || $self->instance_url;
    $url .=  $params{path} if( $params{path} );
    $url .= ( $url =~ m/\?/ ? '&' : '?' )  . $params{options} if( $params{options} );

    my $request = HTTP::Request->new(
        $params{method},
        $url,
        $params{headers},
        );
    $request->content( $params{body} ) if( $params{body} );

    $self->log->debug( "Requesting: " . $url );
    $self->log->trace( "Request:\n" . Dump( $request ) ) if $self->log->is_trace;

    my $response;
    my $retry = 1;
    my $try_count = 0;
    do{
        my $retry_delay = $self->default_backoff;
        $try_count++;
        $response = $self->user_agent->request( $request );
        if( $response->is_success ){
            $retry = 0;
        }else{
            # 401 and "session expired" requires fresh token and login
            if( $response->code == 401 ){
                my $data = decode_json( encode( 'utf8', $response->decoded_content ) );
                if( $data->{errorCode} eq 'INVALID_SESSION_ID' ){
                    $self->refresh_access_token;
                    $retry_delay = 0;
                }
            }elsif( grep{ $_ == $response->code } @{ $self->retry_on_status } ){
                $self->log->debug( Dump( $response ) );
                if( $response->code == 429 ){
                    # TODO confirm that this is implimented in SF
                    # if retry-after header exists and has valid data use this for backoff time
                    if( $response->header( 'Retry-After' ) and $response->header('Retry-After') =~ /^\d+$/ ) {
                        $retry_delay = $response->header('Retry-After');
                    }
                    $self->log->warn( sprintf( "Received a %u (Too Many Requests) response with 'Retry-After' header... going to backoff and retry in %u seconds!",
                            $response->code,
                            $retry_delay,
                            ) );
                }else{
                    $self->log->warn( sprintf( "Received a %u: %s ... going to backoff and retry in %u seconds!",
                            $response->code,
                            $response->decoded_content,
                            $retry_delay
                            ) );
                }
            }else{
                $retry = 0;
            }

            if( $retry == 1 ){
                if( not $self->max_tries or $self->max_tries > $try_count ){
                    $self->log->debug( sprintf( "Try %u failed... sleeping %u before next attempt", $try_count, $retry_delay ) );
                    sleep( $retry_delay );
                }else{
                    $self->log->debug( sprintf( "Try %u failed... exceeded max_tries (%u) so not going to retry", $try_count, $self->max_tries ) );
                    $retry = 0;
                }
            }
        }
    }while( $retry );

    $self->log->trace( "Last response:\n", Dump( $response ) ) if $self->log->is_trace;
    if( not $response->is_success ){
	$self->log->logdie( "API Error: http status:".  $response->code .' '.  $response->message . ' Content: ' . $response->content);
    }
    if( $response->decoded_content ){
        return decode_json( encode( 'utf8', $response->decoded_content ) );
    }
    return;
}


1;

=back

=head1 COPYRIGHT

Copyright 2015, Robin Clarke 

=head1 AUTHOR

Robin Clarke <robin@robinclarke.net>

Jeremy Falling <projects@falling.se>

