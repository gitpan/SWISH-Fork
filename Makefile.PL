use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME            => 'SWISH::Fork',
    VERSION_FROM    => 'Fork.pm',  # finds $VERSION

    NORECURS        => 1,          # keep it from recursing into subdirectories
    DIR             => [],

    PREREQ_PM       => {
        'SWISH'         => 0,
        'Sys::Signal'   => 0.02,
    },
    
    dist            => {
        COMPRESS        => 'gzip',
        SUFFIX          => 'gz',
        # PREOP         => 'pod2text -80 < Fork.pm > README',
    },
);
