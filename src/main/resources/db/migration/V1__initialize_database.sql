create table songs
(
    id         uuid primary key,
    name       varchar(1024) not null,
    artist     varchar(1024) not null,
    album_name varchar(1024) not null
);
