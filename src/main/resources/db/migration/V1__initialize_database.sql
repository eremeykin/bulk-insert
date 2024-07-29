create table songs
(
    id         uuid,
    name       varchar(1024) not null,
    artist     varchar(1024) not null,
    album_name varchar(1024) not null
);

create table songs_pk
(
    id         uuid primary key,
    name       varchar(1024) not null,
    artist     varchar(1024) not null,
    album_name varchar(1024) not null
);