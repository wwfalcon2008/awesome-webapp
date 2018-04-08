-- schema.sql

drop database if exists awesome;

create database awesome;

use awesome;

grant select, insert, update, delete on awesome.* to 'www-data'@'localhost' identified by 'www-data';

create table users(
  `id` varchar(50) not null,
  `email` varchar(50) not null,
  `passwd` VARCHAR(50) not null,
  `admin` bool not null,
  `name` varchar(50) not null,
  `image` varchar(500) not null,
  `created_at` real not null,
  unique key `idx_email` (`email`),
  key `idx_created_at` (`created_at`),
  PRIMARY KEY (`id`)
) engine=innodb DEFAULT  charset=utf8;

create table blogs(
  `id` varchar(50) not null,
  `user_id` VARCHAR(50) not null,
  `user_name` VARCHAR(50) not null,
  `user_image` VARCHAR(500) not null,
  `name` varchar(50) not null,
  `summary` VARCHAR(200) not NULL,
  `content` MEDIUMTEXT not NULL,
  `created_at` real not null,
  key `idx_created_at` (`created_at`),
  primary key(`id`)
) engine=innodb DEFAULT  charset=utf8;

CREATE TABLE comments (
  `id` varchar(50) not null,
  `blog_id` VARCHAR(50) not null,
  `user_id` VARCHAR(50) NOT NULL,
  `user_name` VARCHAR(50) NOT NULL,
  `user_image` VARCHAR(500) not NULL,
  `content` MEDIUMTEXT not NULL,
  `created_at` real not null,
  key `idx_created_at` (`created_at`),
  PRIMARY KEY (`id`)
) engine=innodb DEFAULT CHARSET=utf8;