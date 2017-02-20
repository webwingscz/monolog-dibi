<?php

namespace Webwings\Monolog;

use Dibi\Connection;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Logger;

class DibiHandler extends AbstractProcessingHandler {

    /**
     * @var bool defines whether the MySQL connection is been initialized
     */
    private $initialized = false;

    /**
     * @var Connection
     */
    protected $db;

    /**
     * @var string the table to store the logs in
     */
    private $table = 'logs';

    /**
     * @var array default fields that are stored in db
     */
    private $defaultfields = array('id', 'channel', 'level', 'message', 'formatted', 'time');

    /**
     * @var string[] additional fields to be stored in the database
     *
     * For each field $field, an additional context field with the name $field
     * is expected along the message, and further the database needs to have these fields
     * as the values are stored in the column name $field.
     */
    private $additionalFields = array();

    /**
     * Constructor of this class, sets the DibiConnectin and calls parent constructor
     *
     * @param Connection $connection            Dibi Connector for the database
     * @param bool|int $level                   Debug level which this handler should store
     * @param bool $table                       Table in the database to store the logs in
     * @param array $additionalFields           Additional Context Parameters to store in database
     * @param bool $bubble
     */
    public function __construct(Connection $connection, $level = Logger::DEBUG, $table = 'logs', $additionalFields = array(), $bubble = true) {
        if (!is_null($connection)) {
            $this->db = $connection;
        }
        $this->table = $table;
        $this->additionalFields = $additionalFields;
        parent::__construct($level, $bubble);
    }

    /**
     * Initializes this handler by creating the table if it not exists
     */
    private function initialize() {

        $this->db->query('CREATE TABLE IF NOT EXISTS %n '
                . '(id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY, '
                . 'channel VARCHAR(180), '
                . 'level INTEGER, '
                . 'message LONGTEXT, '
                . 'formatted LONGTEXT, '
                . 'time INTEGER UNSIGNED, '
                . 'INDEX(channel) USING HASH, '
                . 'INDEX(level) USING HASH, '
                . 'INDEX(time) USING BTREE)', $this->table);

        //Read out actual columns
        $actualFields = array();
        $columns = $this->db->query("SHOW columns FROM %n", $this->table)->fetchAll();
        foreach ($columns as $col) {
            $actualFields[] = $col->Field;
        }

        //Calculate changed entries
        $removedColumns = array_diff(
                $actualFields, $this->additionalFields, $this->defaultfields
        );
        $addedColumns = array_diff($this->additionalFields, $actualFields);

        //Remove columns
        foreach ($removedColumns as $c) {
            $this->db->query('ALTER TABLE %n DROP %n', $this->table, $c);
        }


        //Add columns
        foreach ($addedColumns as $c) {
            $this->db->query('ALTER TABLE %n add %n TEXT NULL DEFAULT NULL;', $this->table, $c);
        }


        // merge default and additional field to one array
        $this->defaultfields = array_merge($this->defaultfields, $this->additionalFields);

        $this->initialized = true;
    }

    /**
     * Writes the record down to the log of the implementing handler
     *
     * @param array $record
     * @return void
     */
    protected function write(array $record) {
        if (!$this->initialized) {
            $this->initialize();
        }

        $insert = [];
        foreach ($this->defaultfields as $field) {
            $insert[$field] = isset($record[$field]) ? $record[$field] : NULL;
        }

        $this->db->insert($this->table, $insert)->execute();
    }

}
