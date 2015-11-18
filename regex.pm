use v6;

unit module regex;


sub quotemeta ($str is copy) {
    $str ~~ s:g[\W+] = "'$/'";
    return $str;
}

our sub filter-as-regex ($filter) {
    return /^ <!before '$'>/ if $filter eq '#';
    return /^ '/'/           if $filter eq '/#';

    my $regex = '^';
    my $anchor = True;
    $regex ~= "<!before '\$'>" if $filter ~~ /^\+/;
    my @parts = $filter.comb(/ '/#' | '/' | <-[/]>+ /);
    for @parts {
        when '/#' { $anchor = False; last }
        when '/' { $regex ~= '\\/' }
        when '+' { $regex ~= '<-[/]>*' }
        default { $regex ~= quotemeta $_ }
    }
    $regex ~= '$' if $anchor;

#    say $regex;
    return /<$regex>/;
}

#say filter-as-regex("foo/+/bar/");
