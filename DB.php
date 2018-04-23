<?PHP
namespace Interdose;

use PDO;

if (!class_exists('Interdose\DB'))  {

/**
 * Interdose DB MySQL
 *
 * Basic PDO functionality enhanced it with some additional features, e.g. caching and LINQ inspired database queries.
 *
 * @author Dominik Deobald
 * @version 1.5.0-pre
 * @package Interdose\DB
 * @date 2017-03-09 15:38
 * @copyright Copyright (c) 2012-2017, Dominik Deobald / Interdose Ltd. & Co KG
 */

/**
 * Base MySQL class. This is the one the user needs to create an instance of to use the MySQL functionality.
 *
 *
 * @package Interdose\DB
 */
class DB {
	protected $conn;
	protected $connName;
	protected $connPersist;
	public $connType;
	protected $connSettings;

	protected static $DBs = array();

	protected $can_quote;

	public static $Cache = null;

	private $batch;
	private static $globalbatch;

	public static $stats = array('db-connect' => array(), 'db-read' => 0, 'db-write' => 0, 'cache-read' => 0, 'cache-write' => 0, 'cache-hit' => 0, 'db-time' => 0, 'cache-time' => 0, 'queries' => array());
	public static $logLevel = 0;

	public function __construct($connect, $persistent = null) {
		$this->conn = null;
		$this->connName = $connect;
		$this->connPersist = $persistent;
		$this->can_quote = false;
		$this->batch = null;

		$_DBCONFIG = &$GLOBALS['_CONFIG']['database'];

		if (!isset($_DBCONFIG[$this->connName]) && isset($GLOBALS['_CONFIG'][$this->connName]['database']))
			$_DBCONFIG[$this->connName] = &$GLOBALS['_CONFIG'][$this->connName]['database'];

		if (!isset($_DBCONFIG[$this->connName])) {
			throw new \Exception('Database configuration missing');
		}

		if ($this->connName == 'user'    && !isset($_DBCONFIG['user']   )) $this->connName = 'session';
		if ($this->connName == 'session' && !isset($_DBCONFIG['session'])) $this->connName = 'main';

		$this->ConnSettings = &$_DBCONFIG[$this->connName];

		if (!isset($this->ConnSettings['user'])) $this->ConnSettings['user'] = $_DBCONFIG['main']['user'];
		if (!isset($this->ConnSettings['pass'])) $this->ConnSettings['pass'] = $_DBCONFIG['main']['pass'];

		list($this->connType,) = explode(':', $this->ConnSettings['dsn'], 2);

		if ($this->connType == 'http' || $this->connType == 'https') {
			$this->connType = $this->ConnSettings['type'] ?: 'mysql';
		}
	}

	public function exec($sql, $placeholders = array(), $cache_handle = null) {
		if (!is_array($placeholders)) {
            $cache_handle = $placeholders;
            $placeholders = array();
        }
        foreach ($placeholders as $k => $v) {
            $sql = str_replace('{{raw:' . $k . '}}', $v, $sql);
            $sql = str_replace('{{json:' . $k . '}}', $this->prep(json_encode($v)), $sql);
            $sql = str_replace('{{:' . $k . '}}', $this->prep($v), $sql);
        }

		$DB = $this->connectDB();

		self::$stats['db-write']++;
		$mt = microtime(true);
		$res = $DB->exec($sql);
		$mt = microtime(true) - $mt;
		self::$stats['db-time'] += $mt;

		if (!is_null(self::$Cache) && !is_null($cache_handle)) {
			if (!is_array($cache_handle)) $cache_handle = array($cache_handle);
			$mt = microtime(true);
			foreach ($cache_handle as $h) {
				self::$Cache->delete($h);
			}
			self::$stats['cache-time'] += microtime(true) - $mt;
			self::$stats['cache-write'] += count($cache_handle);
		}

		if (self::$logLevel > 0) {
			if (self::$logLevel == 1 && strlen($sql) > 350) $sql = substr($sql, 0, 250) . '[...]';
			$i = count(self::$stats['queries']);
			self::$stats['queries'][$i] = array(
				'db' => $this->connName,
				'sql' => $sql,
				'time' => $mt,
				'rows' => $res,
			);
		}

		return $res;
	}

	public function execBatch($sql, $cache_handle = null, $continue_on_error = false) {
		if (is_array($sql)) {
			$queries = &$sql;
		} else {
			$sql = str_replace("\r", "\n", $sql);

			while (($p = strpos($sql, '/*')) !== false) {
				$pp = strpos($sql, '*/');
				if ($pp === false) $pp = strlen($sql);
				$sql = substr($sql, 0, $p) . substr($sql, $pp + 2);
			}

			while (($p = strpos($sql, '--')) !== false) {
				$pp = strpos($sql, "\n");
				if ($pp === false) $pp = strlen($sql);
				$sql = substr($sql, 0, $p) . substr($sql, $pp + 1);
			}

			$queries = preg_split("/;+(?=([^'|^\\\']*['|\\\'][^'|^\\\']*['|\\\'])*[^'|^\\\']*[^'|^\\\']$)/", $sql);
		}

		$res = array();

		foreach ($queries as $query){
			if (strlen(trim($query)) > 0) {
				$res[] = array('query' => $query, 'res' => $this->exec($query, $cache_handle));
			}
			$cache_handle = null;
		}

		return $res;
	}

	public function startBatch($batch) {
		$this->batch = $batch;
	}

	public function endBatch() {
		$this->batch = null;
	}

	public static function startGlobalBatch($batch) {
		self::$globalbatch = $batch;
	}

	public static function endGlobalBatch() {
		self::$globalbatch = null;
	}

	public function query($sql, $placeholders = array(), $cache_handle = null, $cacheable = 120) {
        if (!is_array($placeholders)) {
            $cacheable = $cache_handle;
            $cache_handle = $placeholders;
            $placeholders = array();
        }
        foreach ($placeholders as $k => $v) {
            $sql = str_replace('{{raw:' . $k . '}}', $v, $sql);
            $sql = str_replace('{{json:' . $k . '}}', $this->prep(json_encode($v)), $sql);
            $sql = str_replace('{{:' . $k . '}}', $this->prep($v), $sql);
        }
		if ($cache_handle === null && $this->batch !== null) $cache_handle = $this->batch . '|' . sha1($sql);
		if ($cache_handle === null && self::$globalbatch !== null) $cache_handle = self::$globalbatch . '|' . sha1($sql);
		return new DB_Resultset($this, $sql, $cache_handle, $cacheable);
	}

	public function queryX($sql) {
		$DB = $this->connectDB();

		self::$stats['db-read']++;
		$mt = microtime(true);
		$res = $DB->query($sql);
		$mt = microtime(true) - $mt;
		self::$stats['db-time'] += $mt;

		if (self::$logLevel > 0) {
			if (self::$logLevel == 1 && strlen($sql) > 350) $sql = substr($sql, 0, 250) . '[...]';
			$i = count(self::$stats['queries']);
			self::$stats['queries'][$i] = array(
				'db' => $this->connName,
				'sql' => $sql,
				'time' => $mt,
				'rows' => $res->rowCount(),
			);
			if (self::$logLevel >= 10) {
				self::$stats['queries'][$i]['res'] = $res->fetchAll(PDO::FETCH_ASSOC);
			}
		}

		return $res;
	}

	public function select($columns = null) {
		$res = new DB_Query_Select($this);

		if ($columns !== null) {
			$res->columns($columns);
		}

		return $res;
	}

	public function insert($flags = array()) {
		return new DB_Query_Insert($this, $flags);
	}
	public function replace($flags = array()) {
		$flags[] = 'REPLACE';
		return new DB_Query_Insert($this, $flags);
	}

	public function update() {
		return new DB_Query_Update($this);
	}

	public function delete() {
		return new DB_Query_Delete($this);
	}

	public function beginTransaction() {
		return $this->connectDB()->beginTransaction();
	}

	public function commit() {
		return $this->connectDB()->commit();
	}

	public function getAttribute($attribute) {
		return $this->connectDB()->getAttribute($attribute);
	}

	public function getAvailableDrivers() {
		throw new \Exception('Not Implemented', 501);
	}

	public function lastInsertId() {
		return $this->connectDB()->lastInsertId();
	}

	public function prepare($statement, $driver_options = null) {
		return $this->connectDB()->prepare($statement, $driver_options);
	}

	public function quote($str, $param_type = PDO::PARAM_STR) {
		return $this->connectDB()->quote($str, $param_type);
	}

	public function prep($inval, $if_null = 'null', $if_empty = "''", $param_type = PDO::PARAM_STR) {
		if (is_array($inval)) {
			foreach ($inval as $k => $v) {
				$inval[$k] = $this->prep($v, $if_null, $if_empty, $param_type);
			}
			return $inval;
		} elseif ($inval instanceOf \Interdose\DB\iFilterFunction) {
			return $inval->codeSQL($this);
		} elseif (is_null($inval)) {
			return $if_null;
		} elseif (is_int($inval) || $inval == '0') {
			return $inval;
		} elseif (empty($inval)) {
			return $if_empty;
		} else {
			if ($this->can_quote) {
				$res = $this->connectDB()->quote($inval);
				if ($res !== false) return $res;
				$this->can_quote = false;
			}
			if (!$this->can_quote) {
				switch ($this->connType) {
					case 'sqlsrv':	return " N'" . self::ms_escape_string($inval) . "' ";

					default:		if (function_exists('mysql_escape_string')) {
										return " '" . mysql_escape_string($inval) . "' ";
									} else {
										return " '" . self::mysql_escape_string($inval) . "' ";
									}
				}
			}
		}
	}

	public function equals($value, $param_type = PDO::PARAM_STR, $mode = '') {
		if (is_null($value)) {
			return ' IS ' . $mode . ' NULL ';
		} elseif (is_array($value)) {
			return ' ' . $mode . ' IN (' . implode(',', $this->prep($value, "''", "''", $param_type)) . ') ';
        } else {
			$mode = ($mode == '')?' = ':' <> ';
			return $mode . $this->prep($value, "''", "''", $param_type) . ' ';
		}
	}

	public function equals_not($value, $param_type = PDO::PARAM_STR) {
		return $this->equals($value, $param_type, 'NOT');
	}

	public function rollBack() {
		return $this->connectDB()->rollBack();
	}

	public function setAttribute($attribute, $value = PDO::PARAM_STR) {
		return $this->connectDB()->setAttribute($attribute, $value);
	}

	public function sqliteCreateAggregate() {
		throw new Exception('Not Implemented', 501);
	}

	public function sqliteCreateFunction() {
		throw new Exception('Not Implemented', 501);
	}

	protected function connectDB() {
		if (is_null($this->conn)) {
			$mt = microtime(true);

			if (!isset(self::$DBs[$this->connName])) {
				self::$stats['db-connect'][$this->connName] = array('connected' => 1);

				if (!isset($this->ConnSettings['persistent'])) $this->ConnSettings['persistent'] = false;

				if (is_null($this->connPersist)) $this->connPersist = ($this->ConnSettings['persistent'] === true);

				if (substr($this->ConnSettings['dsn'], 0, 4) == 'http') {
					self::$DBs[$this->connName] = new DB_MySQL_Remote(
						$this->ConnSettings['dsn'],
						$this->ConnSettings['user'],
						$this->ConnSettings['pass'],
						array()
					);
					$this->connType = $this->ConnSettings['type'] ?: 'mysql';
				} elseif (substr($this->ConnSettings['dsn'], 0, 6) == 'mysql:') {
					self::$DBs[$this->connName] = new PDO(
						$this->ConnSettings['dsn'],
						$this->ConnSettings['user'],
						$this->ConnSettings['pass'],
						array(
							PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
							PDO::ATTR_PERSISTENT => $this->connPersist,
							PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
						)
					);

					$this->connType = 'mysql';
					if (preg_match("~charset=([a-u0-9]+)(;|$)~i", $this->ConnSettings['dsn'], $matches)){
						// use custom character set if provided in DSN
						self::$DBs[$this->connName]->exec('SET CHARACTER SET ' . $matches[1]);
						self::$DBs[$this->connName]->exec('SET NAMES ' . $matches[1]);
					} else {
						// default character set
						self::$DBs[$this->connName]->exec('SET CHARACTER SET utf8');
						self::$DBs[$this->connName]->exec('SET NAMES utf8');
					}
					self::$DBs[$this->connName]->exec("SET time_zone = '+0:00'");
				} elseif (substr($this->ConnSettings['dsn'], 0, 7) == 'sqlsrv:') {
					self::$DBs[$this->connName] = new PDO(
						$this->ConnSettings['dsn'],
						$this->ConnSettings['user'],
						$this->ConnSettings['pass'],
						array(
							PDO::SQLSRV_ATTR_DIRECT_QUERY => true,
							PDO::ATTR_PERSISTENT => $this->connPersist,
							PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
						)
					);

					$this->connType = 'sqlsrv';
				}
			}

			$this->conn = self::$DBs[$this->connName];
			$this->can_quote = true;

			self::$stats['db-connect'][$this->connName]['time'] = microtime(true) - $mt;
		}

		return $this->conn;
	}

	public static function addConnection($connect, $host, $dbname, $user, $pass) {
		$GLOBALS['_CONFIG']['database'][$connect] = array(
			'dsn' => 'mysql:dbname=' . $dbname . ';host=' . $host,
			'user' => $user,
			'pass' => $pass,
		);
	}

	public static function direct($host, $dbname, $user, $pass, $persist = false) {
		$connect = 'dbconnect_' . count($GLOBALS['_CONFIG']['database']);
		self::addConnection($connect, $host, $dbname, $user, $pass);
		return new self($connect, $persist);
	}

	// http://stackoverflow.com/questions/574805/how-to-escape-strings-in-sql-server-using-php
	public static function mssql_escape($data) {
		if(is_numeric($data))
			return $data;
		$unpacked = unpack('H*hex', $data);
		return '0x' . $unpacked['hex'];
	}

	public static function ms_escape_string($data) {
		if ( !isset($data) or empty($data) ) return '';
		if ( is_numeric($data) ) return $data;

		$non_displayables = array(
			'/%0[0-8bcef]/',            // url encoded 00-08, 11, 12, 14, 15
			'/%1[0-9a-f]/',             // url encoded 16-31
			'/[\x00-\x08]/',            // 00-08
			'/\x0b/',                   // 11
			'/\x0c/',                   // 12
			'/[\x0e-\x1f]/'             // 14-31
		);
		foreach ( $non_displayables as $regex )
			$data = preg_replace( $regex, '', $data );
		$data = str_replace("'", "''", $data );
		return $data;
    }

	public static function mysql_escape_string($inval) {
		return str_replace(array('\\', "\0", "\n", "\r", "'", '"', "\x1a"), array('\\\\', '\\0', '\\n', '\\r', "\\'", '\\"', '\\Z'), $inval);
	}

	public static function is_assoc($array) {
	  return (bool)count(array_filter(array_keys((array) $array), 'is_string'));
	}
}



class DB_MySQL_Remote {
	protected $url, $user, $secret, $param;
	protected $lastInsertId;
	protected $http;

	public function __construct($url, $user, $secret, $param = array()) {
		$this->url = $url;
		$this->user = $user;
		$this->secret = $secret;

		$this->param = array_merge(
			array(),
			$param
		);

		if (!class_exists('\Interdose_HTTP_Request')) require 'Interdose/HTTP/Request.php';
	}

	public function query($sql) {
		$data = $this->call($sql, 'Q');
		return new DB_Resultset_Cached($data['Result']);
	}

	public function exec($sql) {
		$data = $this->call($sql, 'X');

		$this->lastInsertId = $data['lastInsertId'];
		return $data['Result'];
	}

	protected function call($sql, $mode) {
		$time = time();
		$sig = sha1($time . $sql . $this->secret . $this->user);

		$url = $this->url . '?sig=' . $sig . '&user=' . $this->user . '&ts=' . $time . '&m=' . $mode;

		$h = new \Interdose_HTTP_Request($url);
		$h->post($sql);

		if ($h->HTTP_Code != 200) throw new Exception('Remote DB Error: ' . $h->Result, $h->HTTP_Code);

		$res = json_decode($h->Result, true);

		if (!empty($res['Exception']['Message'])) throw new \Exception($res['Exception']['Message'], $res['Exception']['Code']);

		return $res;
	}

	public function beginTransaction() {
		throw new Exception('Not Implemented', 501);
	}

	public function commit() {
		throw new Exception('Not Implemented', 501);
	}

	public function getAttribute() {
		throw new Exception('Not Implemented', 501);
	}

	public function lastInsertId() {
		return $this->lastInsertId;
	}

	public function quote() {
		return false;
	}

	public function rollBack() {
		throw new Exception('Not Implemented', 501);
	}

	public function setAttribute() {
		throw new Exception('Not Implemented', 501);
	}

}

abstract class DB_Query {
	protected $tables;
	protected $columns;
	protected $where;
	protected $orderby;
	protected $limit;

	protected $filters;
	protected $data;

	protected $DB;

	protected $last_table;
	protected $first_table;

	protected $canUseColumnAlias, $canUseMultiTable;

	abstract protected function buildQuery();

	public function __construct($DB, $canUseColumnAlias = true, $canUseMultiTable = true) {
		$this->tables = array();
		$this->columns = array();
		$this->where = array();
		$this->orderby = array();
		$this->limit = array(null, null);

		$this->filters = array();
		$this->data = array();

		$this->last_table = null;

		$this->DB = $DB;
		$this->canUseColumnAlias = $canUseColumnAlias;
		$this->canUseMultiTable = $canUseMultiTable;
	}

	public function from($table, $alias = null) {
		return $this->join($table, $alias);
	}

	protected function quoteObj($obj) {
		switch($this->DB->connType) {
			case 'sqlsrv':	return '[' . str_replace('.', '].[', $obj) . ']';
			default:		if ($obj == '*') {
								return $obj;
							} else {
								return '`' . $obj . '`';
							}
		}
	}

	public function columns($cols) {
		if (!is_array($cols)) {
			if (strpos($cols, ',') !== false) {
				$cols = explode(',', $cols);
			} else {
				$cols = func_get_args ();
			}
		} else {
			if (isset($cols['column'])) {
				$cols = array($cols);
			}
		}

		foreach ($cols as $v) {
			$cc = $this->getDetailedColumn($v, null, null, true);
			$this->columns[$cc['full']] = $cc;
		}

		return $this;
	}

	public function order($col, $order = 'ASC', $overwrite = false) {
		if ($overwrite) $this->orderby[] = array();

		$order = strtoupper($order);

		if ($order != 'DESC') $order = 'ASC';

		$cc = $this->getDetailedColumn($col, null, null, false);
		$this->orderby[] = $cc . ' ' . $order;

		return $this;
	}

	public function orderDirect($override, $overwrite = false) {
		if ($overwrite) $this->orderby[] = array();

		$this->orderby[] = $override;

		return $this;
	}

	public function left_outer_join($table, $alias, $id_column = null, $relation_id_column = null) {
		return $this->join($table, $alias, $id_column, $relation_id_column, 'LEFT OUTER');
	}

	public function join($table, $alias, $id_column = null, $relation_id_column = null, $join_type = 'INNER') {
		$database = null;

		if (is_array($table)) {
			if (isset($table['database']) && isset($table['table'])) {
				$table = $table['table'];
				$database = $table['database'];
			} else {
				list($database, $table) = $table;
			}
		} elseif (strpos($table, '.') !== false) {
			list($database, $table) = explode('.', $table);
		}

		$alias = $this->getAlias($alias, $table);
		$this->tables[$alias] = array('table' => $table, 'database' => $database);

		if (!is_null($id_column)) {
			$this->tables[$alias]['id_column'] = $id_column;

			if (is_null($relation_id_column)) {
				$this->tables[$alias]['join_to'] = $this->quoteObj($this->last_table) . '.' . $this->quoteObj($id_column);
			} else {
				$this->tables[$alias]['join_to'] = $this->getDetailedColumn($relation_id_column);
			}

			$this->tables[$alias]['join_type'] = $join_type;
		}

		$this->last_table = $alias;
		return $this;
	}

	public function getSQL() {}
	public function __toString() {
		return $this->getSQL();
	}

	// http://stackoverflow.com/questions/173400/php-arrays-a-good-way-to-check-if-an-array-is-associative-or-sequential/4254008#4254008
	protected static function array_is_assoc($array) {
		return (bool)count(array_filter(array_keys($array), 'is_string'));
	}

	function values($data) {
		if (self::array_is_assoc($data)) {
			$this->data = array_merge($this->data, array($data));
		} else {
			$this->data = array_merge($this->data, $data);
		}
		return $this;
	}

	function row($data) {
		return $this->values(array($data));
	}

	function filters($column, $rules = null) {
		if (is_null($rules) && is_array($column)) {
			foreach ($column as $c => $r) $this->filters($c, $r);
		} else {
			if (!is_array($rules)) $rules = array('function' => $rules);
			if (is_array($this->filters[$column])) {
				$this->filters[$column] = array_merge($this->filters[$column], $rules);
			} else {
				$this->filters[$column] = $rules;
			}
		}

		return $this;
	}

	public function where () {
		$args = func_get_args ();

		switch (count($args)) {
			case 1:
				$w = func_get_arg(0);

				if (DB::is_assoc($w)) {
					foreach ($w as $k => $v) {
						$this->_addWhere($k, '=', $v);
					}
				} else if (is_array($w)) {
					foreach ($w as $k => $v) {
						if (is_array($v)) {
							if (count($v) == 3) {
								$this->_addWhere($v[0], $v[1], $v[2]);
							} else {
								$this->_addWhere($v[0], '=', $v[1]);
							}
						} else {
							$this->where[] = $v;
						}
					}
				} else {
					$this->where[] = $w;
				}
				break;

			case 2:
				$this->_addWhere(func_get_arg(0), '=', func_get_arg(1));
				break;

			case 3:
				$this->_addWhere(func_get_arg(0), func_get_arg(1), func_get_arg(2));
				break;
		}

		return $this;
	}

	private function _addWhere($k, $op, $v = null) {
		switch (strtolower($op)) {
			case '=': case '==':
				$this->where[] = $this->getDetailedColumn($k) . $this->DB->equals($v);
				break;

			case '!=': case '<>': case '!': case 'not': case 'is not':
				$this->where[] = $this->getDetailedColumn($k) . $this->DB->equals_not($v);
				break;

			case '<': case '>': case 'like':
				$this->where[] = $this->getDetailedColumn($k) . ' ' . $op . ' ' . $this->DB->prep($v);
				break;

			default:
				if ($v === null) {
					$this->where[] = $this->getDetailedColumn($k) . $this->DB->equals($op);
				} else {
					throw new \Exception('WHERE not understood');
				}
				break;
		}
	}

	public function limit() {
		switch (func_num_args()) {
			case 1:
				$p = func_get_arg(0);
				if (is_array($p)) {
					$this->limit = array(intval($p[0]), intval($p[1]));
				} else {
					$this->limit = array(0, intval($p));
				}
				break;

			case 2:
				$this->limit = array(intval(func_get_arg(0)), intval(func_get_arg(1)));
				break;

			default:
				throw new Exception('Limit only takes one or two parameters.');
		}

		return $this;
	}

	protected function getAlias($alias, $table) {
		if (empty($alias)) {
			$p = (strrpos($table, '.') ?: -1);
			$tt = explode('_', strtoupper(substr($table, $p + 1)));
			foreach ($tt as $k) $alias .= substr($k, 0, 1);
			if (isset($this->tables[$alias])) $alias = $alias . '2';
			if (isset($this->tables[$alias])) $alias = 'tbl' . count($this->tables);
		} else {
			if (isset($this->tables[$alias])) throw new \Exception('Table alias already used.');
		}

		return $alias;
	}

	protected function getDetailedColumn($column, $table = null, $alias = null, $withDetails = false) {
		if (is_array($column)) {
			if (!empty($column['table'])) {
				$res = array(
					'table' => $column['table'],
					'column' => $column['column'],
				);
			} elseif (!empty($column[0]) && !empty($column[1])) {
				$res = array(
					'table' => $column[0],
					'column' => $column[1],
				);
			} else {
				$res = array(
					'table' => $this->last_table,
					'column' => $column['column'],
				);
			}
		} else {
			if (strpos($column, '.') !== false) {
				$res = array();
				list($res['table'], $res['column']) = explode('.', $column);
			} elseif (is_null($table)) {
				$res = array(
					'table' => $this->last_table,
					'column' => $column,
				);
			} else {
				$res = array(
					'table' => $table,
					'column' => $column,
				);
			}
		}

		if (!is_null($alias)) {
			$res['alias'] = $alias;
		} elseif(is_array($column)) {
			$res['alias'] = empty($column['alias'])?null:$column['alias'];
		} else {
			$res['alias'] = null;
		}

		if (!$withDetails) {
			if ($this->canUseMultiTable && !empty($res['table'])) {
				return $this->quoteObj($res['table']) . '.' . $this->quoteObj($res['column']);
			} else {
				return $this->quoteObj($res['column']);
			}
		} else {
			if ($this->canUseMultiTable && !empty($res['table'])) {
				$res['escaped'] = $this->quoteObj($res['table']) . '.' . $this->quoteObj($res['column']);
			} else {
				$res['escaped'] = $this->quoteObj($res['column']);
			}
			if ($this->canUseColumnAlias) {
				$res['full'] = $res['escaped'] . (!empty($res['alias'])?(' AS ' . $this->quoteObj($res['alias'])):'');
			} else {
				$res['full'] = $res['escaped'];
			}
			return $res;
		}
	}

	protected function buildTableList($reqired = 'FROM') {
		$froms = array();
		$joins = array();
		$comma = '';

		foreach ($this->tables as $a => $v) {
			$quotedTable = $this->quoteObj($v['table']);
			if (!empty($v['database'])) $quotedTable = $this->quoteObj($v['database']) . '.' . $quotedTable;

			if (empty($v['id_column'])) {
				if ($this->canUseMultiTable) {
					$froms[] = $comma . $quotedTable . ' ' . $this->quoteObj($a);
				} else {
					return $quotedTable;
				}
			} else {
				$joins[] = $v['join_type'] . ' JOIN ' . $quotedTable . ' ' . $this->quoteObj($a) . ' ON ' . $this->quoteObj($a) . '.' . $this->quoteObj($v['id_column']) . ' = ' . $v['join_to'];
			}
		}

		if (empty($froms)) throw new exception('Need to specify "' . $required . '"!');

		return implode(',', $froms) . ' ' . implode(' ', $joins);
	}

	protected function buildWhere() {
		if (count($this->where) > 0) {
			return 'WHERE ' . implode(' AND ', $this->where);
		}
		return '';
	}

	protected function prepareRows($asArray = false) {
		$columns = $this->columns;
//		var_dump($columns);
//		var_dump($this->data);
		if (empty($columns)) {
			$columns = array();
			foreach($this->data[0] as $k => $v) $columns[] = $this->getDetailedColumn($k, null, null, true);
		}
//		var_dump($columns);
		if (empty($columns)) return false;

		$res = array(
			'columns' => array(),
			'values' => array()
		);

		foreach ($columns as $k => $c) {
			$res['columns'][$k] = $c['full'];
		}

		$filters = array();
		foreach ($columns as $c) {
			$c = $c['column'];

			$filters[$c] = array(
				'if_null' => 'null',
				'if_empty' => "''",
				'filter' => null,
				'filter_param' => '',
				'function' => null,
				'type' => PDO::PARAM_STR,
				'maxlen' => null,
			);

			if (!isset($this->filters[$c])) $this->filters[$c] = array();
			if (!isset($filters[$c])) $filters[$c] = array();

			$filters[$c] = array_merge($filters[$c], $this->filters[$c]);

			if (!is_null($filters[$c]['filter']))     $filters[$c]['filter']       = strtoupper($filters[$c]['filter']);
			if (!is_null($filters[$c]['function']))   $filters[$c]['function']     = strtoupper($filters[$c]['function']);
			if (!empty($filters[$c]['filter_param'])) $filters[$c]['filter_param'] = ',' . $filters[$c]['filter_param'];

			if (substr($filters[$c]['function'], -2) == '()') $filters[$c]['function'] = substr($filters[$c]['function'], 0, -2);
		}

		foreach ($this->data as $i => $row) {
			$irow = &$res['values'][$i];

			foreach ($columns as $k => $c) {
				$filtered = false;

				$c = $c['column'];

				// $k = NUMBER of corrent column (0 = leftmost, 1, 2, ...)
				// $c = NAME of current column
				// $row[$c] = SOURCE data from the array the user gave us
				// $irow[$k] = TARGET data that is "imploded" later on.

				if ($row[$c] instanceof \Interdose\DB\iFilterFunction) {
					$irow[$k] = $row[$c]->codeSQL($this->DB, $c);
				} else {
					switch ($filters[$c]['filter']) {
						case 'TRIM':				$filtered = true; $row[$c] = trim($row[$c]);									break;
						case 'UCASE': case 'UPPER': $filtered = true; $row[$c] = strtoupper($row[$c]);								break;
						case 'LCASE': case 'LOWER': $filtered = true; $row[$c] = strtolower($row[$c]);								break;
						case 'LEFT':				$filtered = true; $row[$c] = substr($row[$c], 0, $filters[$c]['filter_param']);	break;
						case 'RIGHT':				$filtered = true; $row[$c] = substr($row[$c], -$filters[$c]['filter_param']);	break;
					}

					switch ($filters[$c]['function']) {
						case 'UUID':
								if (function_exists('uuid')) {
									$irow[$k] = $this->DB->prep(uuid());
								} else {
									$irow[$k] = 'UUID()';
								}
							break;

						case 'CURDATE': case 'CURRENT_DATE':
						case 'CURTIME': case 'CURRENT_TIME':
						case 'NOW': case 'CURRENT_TIMESTAMP':
						case 'UNIX_TIMESTAMP':
						case 'USER': case 'DATABASE':
							$irow[$k] = $filters[$c]['function'] . '()';
							break;

						case 'INC':
							$irow[$k] = $this->quoteObj($c) . ' + 1';
							break;

						case 'DEC':
							$irow[$k] = $this->quoteObj($c) . ' - 1';
							break;

						case 'UNIXTIME_AS_DATE':
							if (!ctype_digit($row[$c])) {
								$irow[$k] = 'null';
							} else {
								$irow[$k] = "'" . date('Y-m-d H:i:s', $row[$c]) . "'";
								if ($this->DB->connType == 'sqlsrv') $irow[$k] = "convert(datetime, " . $irow[$k] . ", 120)";
							}
							break;

						default:
							if (!is_null($filters[$c]['maxlen']))
								if (strlen($row[$c]) > $filters[$c]['maxlen'])
									$row[$c] = substr($row[$c], 0, $filters[$c]['maxlen']);

							$irow[$k] = $this->DB->prep($row[$c], $filters[$c]['if_null'], $filters[$c]['if_empty'], $filters[$c]['type']);
					}

					if (!$filtered && !empty($filters[$c]['filter'])) {
						$irow[$k] = $filters[$c]['filter'] . '(' . $irow[$k] . $filters[$c]['filter_param'] . ')';
					}
				}
			}

			if (!$asArray) $irow = '(' . implode(',', $irow) . ')';
		}

		return $res;
	}
}

/**
 * LINQ style SELECT class.
 *
 * @package Interdose\DB
 */
class DB_Query_Select extends DB_Query {
	protected $distinct;
	function __construct($DB) {
		parent::__construct($DB);
		$this->distinct = '';
	}

	public function values($data) {
		throw new \Exception ('VALUES not allowed for SELECT.');
	}

	public function row($data) {
		throw new \Exception ('ROW not allowed for SELECT.');
	}

	public function filters($column, $rules = NULL) {
		throw new \Exception ('FILTERS not allowed for SELECT.');
	}

	public function distinct($distinct = true) {
		$this->distinct = $distinct ? 'DISTINCT' : '';
		return $this;
	}

	protected function buildQuery($mode = null) {
		$sql = array();
		$limited = ($this->limit[1] !== null);

		$wrap_start = '';
		$wrap_end = '';

		$sql[] = 'SELECT';
		if ($mode !== 'COUNT') $sql[] = $this->distinct;

		if ($limited && $this->limit[0] === 0 && $this->DB->connType == 'sqlsrv') {
			$sql[] = 'TOP ' . $this->limit[1];
			$limited = false;
		}

		if ($mode === 'COUNT') {
			$sql[] = 'COUNT(*) as RowCount FROM';
		} else {
			if (count($this->columns) > 0) {
				$sql[] = implode(',', array_keys($this->columns)) . ' FROM';
			} else {
				$sql[] = '* FROM';
			}
		}

		$sql[] = $this->buildTableList('FROM');
		$sql[] = $this->buildWhere();

		if (count($this->orderby) > 0) {
			$sql[] = 'ORDER BY';
			$sql[] = implode(',', $this->orderby);
		}

		if ($limited) {
			if ($this->DB->connType == 'mysql') {
				$sql[] = 'LIMIT ' . $this->limit[0] . ', ' . $this->limit[1];
			} elseif ($this->DB->connType == 'sqlsrv') {
				// SQLSRV 2012+
				if (count($this->orderby) == 0) $sql[] = 'ORDER BY 1';
				$sql[] = 'OFFSET ' . $this->limit[0] . ' FETCH ' . $this->limit[1];

//				// SQLSRV 2005+
//				SET @start = 120000
//				SET @rowsperpage = 10
//
//				SELECT * FROM
//				(
//				SELECT row_number() OVER (ORDER BY column) AS rownum, column2, column3, .... columnX
//				  FROM   table
//				) AS A
//				WHERE A.rownum
//				BETWEEN (@start) AND (@start + @rowsperpage)
			}
		}

		return $wrap_start . implode(' ', $sql) . $wrap_end;
	}

	public function query($cache_handle = null, $cacheable = 120) {
		return $this->DB->query($this->buildQuery(), $cache_handle, $cacheable);
	}

	public function queryCount($cache_handle = null, $cacheable = 120) {
		return $this->DB->query($this->buildQuery('COUNT'), $cache_handle, $cacheable);
	}

	public function getSQL() {
		return $this->buildQuery();
	}
}

/**
 * LINQ style INSERT class.
 *
 * @package Interdose\DB
 */
class DB_Query_Insert extends DB_Query {
	protected $flags = array();
	protected $insert_mode = 'INSERT';
	protected $duplicate_key_update = array();

	function __construct($DB, $flags = array()) {
		if (empty($flags)) {
			$flags = array();
		} elseif (!is_array($flags)) {
			$flags = array($flags);
		}

		$k = array_search('REPLACE', $flags);
		if ($k === false) $k = array_search('replace', $flags);
		if ($k !== false) {
			$this->insert_mode = 'REPLACE';
			unset($flags[$k]);
		}

		foreach ($flags as $flag) {
			$this->flags[strtoupper($flag)] = $flag;
		}
		parent::__construct($DB, false, false);
	}

	public function into($table) {
		if (!empty($this->tables)) throw new Exception ('Only one table allowed for INSERT.');
		parent::join($table, 'T');

		return $this;
	}

	public function ignore() {
		$this->flags['IGNORE'] = 'IGNORE';
		return $this;
	}

	public function from($table, $alias = NULL) {
		throw new \Exception ('FROM not allowed for INSERT.');
	}

	public function join($table, $alias, $id_column = NULL, $relation_id_column = NULL, $join_type = 'INNER') {
		throw new \Exception ('JOIN not allowed for INSERT.');
	}

	public function on_duplicate_key_update_with_value($keys) {
		if (!is_array($keys)) $keys = array($keys);

		foreach ($keys as $key) {
			$this->duplicate_key_update[] = $key . ' = VALUES(' . $key . ')';
		}

		return $this;
	}

	protected function buildQuery($flags = array()) {
		$sql = array();

		$data = $this->prepareRows(false);
		if ($data == false) return false; // Nothing to do.

		if (empty($this->tables['T']['table'])) throw new exception('Need to specify "FROM"!');

		$sql[] = $this->insert_mode . ' ' . implode(' ', $flags) . ' INTO ' . $this->quoteObj($this->tables['T']['table']) . ' (';
		$sql[] = implode(',', $data['columns']);
		$sql[] = ') VALUES';
		$sql[] = implode(',', $data['values']);

		if (!empty($this->duplicate_key_update)) {
			$sql[] = 'ON DUPLICATE KEY UPDATE';
			$sql[] = implode(',', $this->duplicate_key_update);
		}

//		print_r($sql);

		return implode(' ', $sql);
	}

	public function getSQL() {
		return $this->buildQuery($this->flags);
	}

	public function exec($cache_handle = null) {
		return $this->DB->exec($this->buildQuery($this->flags), $cache_handle);
	}
}

/**
 * LINQ style INSERT class.
 *
 * @package Interdose\DB
 */
class DB_Query_Update extends DB_Query {
	function __construct($DB) {
		$this->data = array();
		$canUseMultiTable = ($DB->connType == 'mysql');
		parent::__construct($DB, false, $canUseMultiTable);
	}

	public function table($table) {
		return parent::join($table, 'T');
	}

	function values($data) {
		if (count($this->data)) throw new Exception ('Can only use one ROW for UPDATE.');
		return parent::values(array($data));
	}

	public function from($table, $alias = NULL) {
		throw new Exception ('FROM not allowed for UPDATE. Use table() instead.');
	}

	protected function buildQuery() {
		$sql = array();
		$limited = ($this->limit[1] !== null);

		$data = $this->prepareRows(true);
		if ($data == false) return false; // Nothing to do.

		$sql[] = 'UPDATE';

		if ($limited && $this->limit[0] === 0 && $this->DB->connType == 'sqlsrv') {
			$sql[] = 'TOP ' . $this->limit[1];
			$limited = false;
		}

		$sql[] = $this->buildTableList('TABLE');
		$sql[] = 'SET';

		$row = array();

		foreach ($data['columns'] as $k => $col) {
			$row[] = $col . ' = ' . $data['values'][0][$k];
		}

		$sql[] = implode(',', $row);
		$sql[] = $this->buildWhere();

		if (count($this->orderby) > 0) {
			$sql[] = 'ORDER BY';
			$sql[] = implode(',', $this->orderby);
		}


		if ($limited) {
			if ($this->DB->connType == 'mysql') {
				$sql[] = 'LIMIT ' . $this->limit[0] . ', ' . $this->limit[1];
			} elseif ($this->DB->connType == 'sqlsrv') {
				// SQLSRV 2012+
				if (count($this->orderby) == 0) $sql[] = 'ORDER BY 1';
				$sql[] = 'OFFSET ' . $this->limit[0] . ' FETCH ' . $this->limit[1];
			}
		}

//		print_r($data);
//		print_r($sql);

		return implode(' ', $sql);
	}

	public function getSQL() {
		return $this->buildQuery();
	}


	public function exec($cache_handle = null) {
		return $this->DB->exec($this->buildQuery(), $cache_handle);
	}
}

/**
 * LINQ style DELETE class.
 *
 * @package Interdose\DB
 */
class DB_Query_Delete extends DB_Query {
	function __construct($DB) {
		parent::__construct($DB, false, false);
	}

	public function from($table, $alias = NULL) {
		if (!empty($this->tables)) throw new Exception ('Only one table allowed for DELETE.');
		parent::join($table, 'T');

		return $this;
	}

	public function columns($cols) {
		throw new Exception ('COLUMNS not allowed for DELETE.');
	}

	public function join($table, $alias, $id_column = NULL, $relation_id_column = NULL, $join_type = 'INNER') {
		throw new Exception ('JOIN not allowed for DELETE.');
	}

	public function values($data) {
		throw new Exception ('VALUES not allowed for SELECT.');
	}

	public function row($data) {
		throw new Exception ('ROW not allowed for SELECT.');
	}

	public function filters($column, $rules = NULL) {
		throw new Exception ('FILTERS not allowed for SELECT.');
	}

	protected function buildQuery() {
		$sql = array();

		if (empty($this->tables['T']['table'])) throw new exception('Need to specify "FROM"!');

		$sql[] = 'DELETE FROM';
		$sql[] = $this->tables['T']['table'];
		$sql[] = $this->buildWhere();
//		$sql[] = $this->limit;

		return implode(' ', $sql);
	}

	public function exec($cache_handle = null) {
		return $this->DB->exec($this->buildQuery(), $cache_handle);
	}

	public function getSQL() {
		return $this->buildQuery();
	}
}

/**
 * This class implements the basic functionality of PDOStatement, but it uses an array as data source,
 * not a result returned from database. This is required for caching.
 *
 * @package Interdose\DB
 */
class DB_Resultset_Cached {
	protected $data;
	protected $numRows;
	protected $pointer;
	protected $state;

	protected $bindings;
	protected $bindingsTypes;

	public function __construct($data, $cache_handle = null, $cacheable = 120) {
		$this->data = $data;
		$this->numRows = count($data);
		$this->pointer = -1;
		$this->state = 0;
		$this->bindings = array();
		$this->bindingsTypes = array('NUM' => false, 'ASSOC' => false);

		$this->toCache($cache_handle, $cacheable);
	}

	public static function fromCache($cache_handle, $cacheable = null) {
		if (is_null(DB::$Cache)) return false;

		DB::$stats['cache-read']++;
		$mt = microtime(true);
//		echo ' Reading from Cache: ' . $cache_handle . '<br>';
		$res = DB::$Cache->get($cache_handle, false, $cacheable);
//		echo ' Got from Cache: ' . $res . '<br>';
		DB::$stats['cache-time'] += microtime(true) - $mt;

		if ($res === false) return false;

		DB::$stats['cache-hit']++;

		return new DB_Resultset_Cached($res);
	}

	public function toCache($cache_handle = null, $cacheable = 120) {
		if (is_null($cache_handle) || is_null(DB::$Cache) || $cacheable < 1) return false;

		DB::$stats['cache-write']++;
		$mt = microtime(true);
		DB::$Cache->set($cache_handle, $this->data, 0, $cacheable);
		DB::$stats['cache-time'] += microtime(true) - $mt;

		return true;
	}

	public function bindColumn($column, &$param, $type) {
		$this->bindings[$column] = &$param;
		$this->bindingsTypes[is_numeric($column)?'NUM':'ASSOC'] = true;
		return true;
	}

	public function bindParam($parameter, $variable, $data_type = null, $length = null, $driver_options = null) {
		throw new Exception('Cached Result cannot be prepared statement.');
	}

	public function bindValue($parameter, $value, $data_type) {
		throw new Exception('Cached Result cannot be prepared statement.');
	}

	public function execute($parameters = array()) {
		throw new Exception('Cached Result cannot be prepared statement.');
	}

	public function closeCursor() {
		$this->data = null;
		$this->state = 99;
		return true;
	}

	public function columnCount() {
		return count($this->data[$this->pointer]);
	}

	public function errorCode() {
		return 0;
	}

	public function errorInfo() {
		return '';
	}

	public function fetch($fetch_style = PDO::FETCH_ASSOC, $cursor_orientation = PDO::FETCH_ORI_NEXT, $offset = null) {
		switch ($cursor_orientation) {
			case PDO::FETCH_ORI_NEXT:	$this->pointer++; break;
			case PDO::FETCH_ORI_PRIOR:	$this->pointer--; break;
			case PDO::FETCH_ORI_FIRST:	$this->pointer = 0; break;
			case PDO::FETCH_ORI_LAST:	$this->pointer = $this->numRows - 1; break;
			case PDO::FETCH_ORI_ABS:	$this->pointer = is_null($offset)?0:$offset; break;
			case PDO::FETCH_ORI_REL:	$this->pointer += is_null($offset)?1:$offset; break;

			default:
				if ($cursor_orientation > 10) die('ARE YOU TRYING TO CACHE? WRONG PLACE FOR PARAMETERS!');
				throw new Exception('Selected cursor orientation not implemented.', 501);

		}

		if ($this->pointer >= $this->numRows) {$this->pointer = $this->numRows; return false;}
		if ($this->pointer < 0) {$this->pointer = -1; return false;}

		switch ($fetch_style) {
			case PDO::FETCH_ASSOC:
				return $this->data[$this->pointer];

			case PDO::FETCH_NUM:
				return array_values($this->data[$this->pointer]);

			case PDO::FETCH_BOTH:
				return array_merge($this->data[$this->pointer], array_values($this->data[$this->pointer]));

			case PDO::FETCH_BOUND:
				foreach ($this->bindings as $k => $v) $this->bindings[$k] = null;

				if ($this->bindingsTypes['NUM']) {
					$num = array_values($this->data[$this->pointer]);
					foreach ($num as $k => $v) $this->bindings[$k] = $v;
				}
				if ($this->bindingsTypes['ASSOC']) {
					foreach ($this->data[$this->pointer] as $k => $v) $this->bindings[$k] = $v;
				}
				return true;
				break;

			default: throw new Exception('Selected fetch style not implemented.', 501);
		}
	}

	public function fetchAll($fetch_style = PDO::FETCH_ASSOC, $column_index = null) {
		if ($fetch_style = PDO::FETCH_ASSOC) {
			return $this->data;
		} else {
			throw new Exception('Not Implemented', 501);
		}
	}

	public function fetchColumn($column_index = 0) {
		throw new Exception('Not Implemented', 501);
	}

	public function fetchObject($class_name = 'stdClass', $ctor_args = array()) {
		throw new Exception('Not Implemented', 501);
	}

	public function getAttribute($attribute) {
		return false;
	}

	public function getColumnMeta($column) {
		return false;
	}

	public function nextRowset() {
		throw new Exception('Not Implemented', 501);
	}

	public function rowCount() {
		return count($this->data);
	}

	public function setAttribute($attribute, $value) {
		return false;
	}
}

/**
 * Wrapper for a standard PDOStatement with added functionality for caching.
 *
 * @package Interdose\DB
 */
class DB_Resultset {
	protected $DB;
	protected $sql;
	protected $resultset;

	protected $cache_handle, $cacheable;

	protected $fetchMode, $fetchP1, $fetchP2;

	public function __construct(&$DB, $sql, $cache_handle = null, $cacheable = 120) {
		$this->DB = &$DB;
		$this->sql = $sql;
		$this->resultset = null;
		$this->fetchMode = PDO::FETCH_ASSOC;

		$this->cache_handle = $cache_handle;
		$this->cacheable = $cacheable;
	}

	protected function initRS() {
		if (!is_null($this->resultset)) return 0;

		if ($this->cache_handle !== null) {
			$this->resultset = DB_Resultset_Cached::fromCache($this->cache_handle, $this->cacheable);
			if ($this->resultset !== false) return 2;
		}

		$this->resultset = $this->DB->queryX($this->sql);

		if ($this->cache_handle !== null) {
			if ($this->resultset->rowCount() < 1100) {
				$this->resultset = new DB_Resultset_Cached($this->resultset->fetchAll(PDO::FETCH_ASSOC), $this->cache_handle, $this->cacheable);
				return 3;
			}
		}

		return 1;
	}

	public function getSQL() {
		return $this->sql;
	}

	public function bindColumn($column, &$param, $type) {
		$this->initRS();

		return $this->resultset->bindColumn($column, $param, $type);
	}

	public function bindParam($parameter, $variable, $data_type = null, $length = null, $driver_options = null) {
		$this->initRS();

		return $this->resultset->bindParam($parameter, $variable, $data_type, $length, $driver_options);
	}

	public function bindValue($parameter, $value, $data_type) {
		$this->initRS();

		return $this->resultset->bindValue($parameter, $value, $data_type);
	}

	public function closeCursor() {
		$this->initRS();

		return $this->resultset->closeCursor();
	}

	public function columnCount() {
		$this->initRS();

		return $this->resultset->columnCount();
	}

	public function errorCode() {
		$this->initRS();

		return $this->resultset->errorCode();
	}

	public function errorInfo() {
		$this->initRS();

		return $this->resultset->bindColumn();
	}

	public function execute($parameters = array()) {
		$this->initRS();

		return $this->resultset->execute($parameters);
	}

	public function fetch($fetch_style = null, $cursor_orientation = PDO::FETCH_ORI_NEXT, $offset = 1) {
		if (is_null($fetch_style)) $fetch_style = $this->fetchMode;

		$this->initRS();

		return $this->resultset->fetch($fetch_style, $cursor_orientation, $offset);
	}

	public function fetchAll($fetch_style = null) {
		if (is_null($fetch_style)) $fetch_style = $this->fetchMode;

		$this->initRS();

		return $this->resultset->fetchAll($fetch_style);
	}

	public function fetchColumn($column_index = 0) {
		$this->initRS();

		return $this->resultset->fetchColumn($column_index);
	}

	public function fetchObject($class_name = 'stdClass', $ctor_args = array()) {
		$this->initRS();

		return $this->resultset->fetchObject($class_name, $ctor_args);
	}

	public function getAttribute($attribute) {
		$this->initRS();

		return $this->resultset->getAttribute($attribute);
	}

	public function getColumnMeta($column) {
		$this->initRS();

		return $this->resultset->getColumnMeta($column);
	}

	public function nextRowset() {
		$this->initRS();

		return $this->resultset->nextRowset();
	}

	public function rowCount() {
		$this->initRS();

		return $this->resultset->rowCount();
	}

	public function setAttribute($attribute, $value) {
		$this->initRS();

		return $this->resultset->setAttribute($attribute, $value);
	}

	public function setFetchMode($mode, $p1 = null, $p2 = null) {
		throw new \Exception('Not Implemented', 501);
	}
}

$GLOBALS['_DEBUG']['Ids_DB'] = &DB::$stats;

}




namespace Interdose\DB;

interface iFilterFunction {
	public function codeSQL($DB, $column_name);
}

class VALUES implements iFilterFunction {
	private $column;

	public function __construct($column = null) {
		if (empty($column)) throw new \Exception('parameter column required');
		$this->column = $column;
	}

	public function codeSQL($DB, $column_name) {
		return 'VALUES(' . $this->column . ')';
	}
}

class UNIX_TIMESTAMP implements iFilterFunction {
	private $ts;

	public function __construct($ts = null) {
		$this->ts = $ts;
	}

	public function codeSQL($DB, $column_name) {
		$ts = $DB->prep($this->ts, '', '');
		return 'UNIX_TIMESTAMP(' . $ts . ')';
	}
}

class TO_DAYS implements iFilterFunction {
	private $ts;

	public function __construct($ts = null) {
		$this->ts = $ts;
	}

	public function codeSQL($DB, $column_name) {
		if (empty($this->ts)) {
			$ts = 'NOW()';
		} else {
			$ts = $DB->prep($this->ts, '', '');
		}
		return 'TO_DAYS(' . $ts . ')';
	}
}

class UNHEX implements iFilterFunction {
	private $hexString;
	private $columnParam;

	public function __construct($hexString = null, $columnParam = false) {
		$this->hexString = $hexString;
		$this->columnParam = $columnParam;
	}

	public function codeSQL($DB, $column_name) {
		if ($this->columnParam) {
			$hexString = '`' . $this->hexString . '`';
		} else {
			$hexString = $DB->prep($this->hexString, '', '');
		}
		return 'UNHEX(' . $hexString . ')';
	}
}

class UTC_DATE implements iFilterFunction {
	public function __construct() {
	}

	public function codeSQL($DB, $column_name) {
		return 'UTC_DATE()';
	}
}

class UTC_TIME implements iFilterFunction {
	public function __construct() {
	}

	public function codeSQL($DB, $column_name) {
		return 'UTC_TIME()';
	}
}

class UTC_TIMESTAMP implements iFilterFunction {
	public function __construct() {
	}

	public function codeSQL($DB, $column_name) {
		return 'UTC_TIMESTAMP()';
	}
}

class UUID implements iFilterFunction {
	public function __construct() {
	}

	public function codeSQL($DB, $column_name) {
		return 'UUID()';
	}
}
