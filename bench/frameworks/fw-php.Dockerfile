FROM php:8.4-cli-bookworm
RUN apt-get update -qq \
 && apt-get install -y -qq git unzip libzip-dev libgmp-dev libicu-dev libonig-dev >/dev/null \
 && docker-php-ext-install -j4 pcntl bcmath gmp intl zip sockets >/dev/null \
 && yes '' | pecl install redis >/dev/null \
 && docker-php-ext-enable redis \
 && curl -sS https://getcomposer.org/installer | php -- --quiet --install-dir=/usr/local/bin --filename=composer \
 && echo 'memory_limit=2G' > /usr/local/etc/php/conf.d/memory.ini
