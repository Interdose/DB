# Interdose\\DB
This is an (opinionated) extension to PHP's basic PDO class, enhanced it with some additional features, e.g. caching and LINQ inspired database queries.

# Usage
## Initialize class and configure database connection
Configure database connection using `$_CONFIG` variable:
```php
<?php
  $_CONFIG = array(
    'database' => array(
      'dbidentifier' => array(
        'dsn' => 'mysql:dbname=mydbname;host=localhost',
        'user' => 'username',
        'pass' => 'password',
        'persistent' => false
      )
    )
  );
  // initialize database
  require_once 'DB.php';
  $DB = new \Interdose\DB('dbidentifier');
?>
```

Alternatively, you can initialize the class using the `DB::direct()` function: 
```php
<?php
  $_CONFIG = null;
  $DB = \Interdose\DB::direct("localhost","mydbname","username","password");
?>
```

## Simple Queries
### Select
```php
<?php
  // fetch all rows matching the criteria in an associative array 
  $users = $DB->query('SELECT * FROM users WHERE id = '.$DB->prep($id).';')->fetchAll();
  foreach ($users as $u => $user){
    echo 'Username: '.$user["name"]."\n";
    // ...
  }
?>
```
Note, that `$DB->prep()` will do the whole escaping stuff. Instead of `fetchAll()` you can also use `fetch()` which will return the current row instead of all rows. 
