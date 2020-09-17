tables = {
    rows = {10, 20, 30, 90},
    -- SHOW CHARACTER SET;
    charsets = {'utf8', 'latin1', 'binary'},
    -- partition number
    partitions = {'undef'},
    pk_counts = {'1', '2'}
}

fields = {
    types = {'bigint', 'float', 'double', 'decimal(40, 20)',
        'char(20)', 'varchar(20)', 'enum'},
    sign = {'signed', 'unsigned'}
}

data = {
    numbers = {'null', 'tinyint', 'smallint',
        '12.991', '1.009', '-9.183',
        'decimal',
    },
    strings = {'null', 'letter', 'english'},
}