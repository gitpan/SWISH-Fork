package SWISH::Fork;
use strict;

use vars (qw/$VERSION $errstr @ISA $AUTOLOAD/);

# Plan to change to %FIELDS
use base qw/SWISH/;

use Symbol;     # For creating a locallized file handle
use Sys::Signal ();  # for mod_perl


$VERSION = 0.08;

{
    my %available = (
        prog        => 1,       # swish path (path?)
        indexes     => 1,       # Not writable?
        query       => 1,
        tags        => 1,       # Alias content?
        properites  => 1,
        maxhits     => 1,
        startnum    => 1,
        sort        => 1,
        start_date  => 1,
        end_date    => 1,
        results     => 1,
        headers     => 1,
        timeout     => 1,
        errstr      => 0,
        rawline     => 0,   # as read from pipe
        indexheaders => 0,
    );
    sub _readable{ exists $available{$_[1]} };
    sub _writable{ $available{$_[1]} };
}
        

#------------- public methods -------------------------

# Rewrite so will clone an object.
sub new {
    my $class = shift;
    $class = ref( $class ) || $class;
    
    my %attr = ref $_[0] ? %{$_[0]} : @_ if @_;


    $attr{prog} ||= $attr{path} || '';  # Alias
    
    unless ( $attr{prog} ) {
        $errstr = 'Must specify path to swish binary in $attr{prog}';
        return;
    }

    unless ( -x $attr{prog} ) {
        $errstr = "Swish binary '$attr{prog}' not executable: $!";
        return;
    }


    return bless \%attr, $class;

}


sub errstr {
    my ($self, $message ) = @_;

    if ( ref $self ) {
        $self->{_errstr} = $message if $message;
        return $self->{_errstr};
    }

    $errstr;
}



sub query {
    my $self = shift;
    my %attr = ref $_[0] ? %{$_[0]}
                         : @_ == 1 ? ( query => $_[0] ) : @_;


    # make copy of defaults, and merge in passed parameters.
    my %settings = ( %$self, %attr );

    unless ( $settings{indexes} ) {
        $self->errstr( 'Must specify index files' );
        return;
    }

    my @indexes = ref $settings{indexes} ? @{$settings{indexes}} : ( $settings{indexes} );

    for ( @indexes ) {        
        unless ( -r ) {
            $self->errstr( "Index file '$_' not readable $!" );
            return;
        }
    }

    unless ( $settings{query} ) {
        $self->errstr( 'Must specify query' );
        return;
    }


    unless ( $settings{results} && ref $settings{results} eq 'CODE' ) {
        $self->errstr( "Must specify 'results' callback"  );
        return;
    }

    # This may cause problems if using -d or even both.
    $settings{output_separator} ||= '::';
    

    my @parameters;

    push @parameters, '-w', $settings{query},
                      '-f', @indexes,
                      '-d', $settings{output_separator};

                      

    # add other settings to parameters
    push @parameters, _add_options( \%settings );


    return $self->_fork_swish( \%settings, \@parameters );
}

sub raw_query {
    my $self = shift;
    my @output;
    $self->{_raw} = \@output;
    $self->query( @_ );
    delete $self->{_raw};
    return @output;
}


sub abort_query {
    my ( $self, $errstr ) = @_;
    $self->{_abort} = $errstr || '';
}




#-------------- private methods -----------------------

sub _add_options {
    my $settings = shift;

    my %map = (
        properties  => '-p',
        sort        => '-s',
        maxhits     => '-m',
        tags        => '-t',
        context     => '-t',
        startnum    => '-b',
    );

    my %lookup = reverse %map;
    
    my @options;

    for my $option ( keys %$settings ) {

        next unless my ($switch) = ($map{$option}) || $option =~ /^(-\w)$/;

        push @options, $switch;

        # so you can say -e => undef to just add a switch.
        
        push @options, ref $settings->{$option}
                       ? @{$settings->{$option}}
                       : $settings->{$option}
                           if $settings->{$option};

        # Need to consider if someone uses -d instead of output_seperator                           


    }
    return @options;
}    

            

    

use IO::Handle;

sub _fork_swish {
    my ( $self, $settings, $params ) = @_;

    my $fh = gensym;

    STDOUT->flush;  # flush STDOUT STDERR
    STDERR->flush;  # so child doesn't get copies



    # Fork
    my $child = open( $fh, '-|' );

    unless ( defined $child ) {
        $self->errstr( "Failed to fork: '$!'" );
        return;
    }


    # this is in the child
    exec( $self->{prog}, @$params ) || die "failed to exec '$self->{prog}' $!"
        unless $child;

    
    $self->{_start_time} = time;
    $self->{_handle}     = $fh;
    $self->{_child}      = $child;
    delete $self->{_abort};

    my $hits;


    # Use Sys::Signal under mod_perl to restore Apache's signal handler
    # Should be fixed under perl 5.6.1, but check with mod_perl list to be sure.

    eval {
        local $SIG{__DIE__};
        if ( $settings->{timeout} && $settings->{timeout} =~ /\d+/ ) {
            #$SIG{ALRM} = sub { die "Timeout after $settings->{timeout} seconds\n" };
            my $h = Sys::Signal->set(ALRM => sub { die "Timeout after $settings->{timeout} seconds\n" });
            alarm $settings->{timeout};
        }
        $hits = $self->_read_results( $settings );
        alarm 0 if $settings->{timeout};
    };

    if ( $@ ) {
        $self->errstr( $@ );
        kill( 'HUP', $self->{_child} );
        delete $self->{total_hits} if $self->{total_hits};
    }

    close( $self->{_handle} );
    delete $self->{_handle};
    delete $self->{_child};


    #make method?
    return $hits;
    
}


sub _read_results {
    my ( $self, $settings ) = @_;

    my %headers;
    my $header_done;
    my $error;
    my $eof;


    # Should these be method calls?
    $self->{cur_record} = 0;
    $self->{total_hits} = 0;

    delete $self->{indexheaders} if $self->{indexheaders};



    my $fh = $self->{_handle};

    while (my $line = <$fh> ) {



        # a way to exit;
        die (( $self->{_abort} || 'aborted') . "\n") if exists $self->{_abort};

        chomp $line;

        # Raw output
        if ( $self->{_raw} ) {
            push @{$self->{_raw}}, $line;
            next;
        }

        

        $self->{rawline} = $line;


        # save the headder
        if ( $line =~ /^#/ ) {
            if ( $line =~ /^# ([^:]+):\s+(.+)$/ ) {
                my ( $name, $value ) = ( lc($1), $2 );
                $headers{$name} = $value;
            
                $settings->{headers}->( $self, $name, $value )
                    if $settings->{headers} && ref($settings->{headers}) eq 'CODE';
            }

            next;
        }
        

        # If not a header, then save previous set of headers
        if ( %headers ) {
            $self->{total_hits} += $headers{'Number of hits'} if $headers{'Number of hits'};

            push @{$self->{indexheaders}}, {%headers};
            %headers = ();
        }


        if ( $line =~ /^\d/ ) {          # Starts with a digit then assume it's a result


            $self->{cur_record}++;       # this record


            my ( $score, $file, $title, $size, @properties ) =
                split /$settings->{output_separator}/, $line;


            my %result = (
                score       => $score,
                file        => $file,
                title       => $title,
                size        => $size,
                position    => $self->{cur_record},
                total_hits  => $self->{total_hits},     # doesn't work with multiple indexes
            );

            $result{properties} = \@properties if @properties;

            my $result = SWISH::Results->new( \%result );                


            $settings->{results}->( $self, $result );

            next;
        }



        # Catch errors, but not 'no results' since could be more than one index
        $error = $1 if $line =~ /^err:\s*(.+)/;

        
        # Detect eof;    
        $eof++ if $line =~ m[^\.];       # make sure we get all the results;

        # Should check for unexpected data here.
    }

    # Save the current headers.
    if ( %headers ) {
        $self->{total_hits} += $headers{'Number of hits'} if $headers{'Number of hits'};

        push @{$self->{indexheaders}}, {%headers};
        %headers = ();
    }



    if ( $error && $error ne 'no results' ) {
        $self->errstr( $error );

        return;
    }

    return $self->{cur_record} if $self->{cur_record} && $eof;

    $self->errstr('Failed to find results') if $eof;
    $self->errstr('Failed to find end of results') unless $eof;
    return;
}


sub DESTROY {
}


sub AUTOLOAD {
    my $self = shift;
    no strict "refs";


    my $attribute = $1 if $AUTOLOAD =~ /.*::(\w+)/;
    die "failed to find attribute in autoload '$AUTOLOAD'" unless $attribute;


    return unless $self->_readable( $attribute );

    if ( $self->_writable( $attribute ) ) {

        *{$AUTOLOAD} = sub {
            my $me = shift;

            if ( @_ ) {
                my @params = @_;
                $me->{$attribute} = @_ > 1 ? \@params : $params[0];
            }

            return $me->{$attribute} || undef;
        };
    } else {
        *{$AUTOLOAD} = sub {
            return shift->{$attribute} || undef;
        };
    }

    return $self->$AUTOLOAD( @_ );
    
}
    


1;
__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

SWISH::Fork - Perl extension for accessing the SWISH-E search engine via a fork/exec.

=head1 SYNOPSIS

    use SWISH;

    $sh = SWISH->connect('Fork',
       prog     => '/usr/local/bin/swish-e',
       indexes  => 'index.swish-e',
       results  => sub { print $_[1]->as_string,"\n" },
    );


=head1 DESCRIPTION

This module is a driver for the SWISH search engine using the forked access method.
Please see L<SWISH> for usage instructions.


=head2 REQUIRED MODULES

SWISH - the front-end for module for accessing the SWISH search engine.

Sys::Signal - Use instead of C<local $SIG{ALRM}> to restore signal handlers.  Should be fixed in Perl 5.6.1

Symbol - localized file handles (standard module)


=head1 AUTHOR

Bill Moseley -- moseley@hank.org

=head1 SEE ALSO

L<SWISH>

=cut
