import { Client } from 'pg'
import * as wakkanay from 'wakkanay'
import { Bytes } from 'wakkanay/dist/types/Codables'
import { BatchOperation, Iterator } from 'wakkanay/dist/db/KeyValueStore'

export class PostgreSqlKeyValueStore implements wakkanay.db.KeyValueStore {
  client: Client
  constructor(client: Client) {
    this.client = client
  }
  static async open(dbName: Bytes): Promise<PostgreSqlKeyValueStore> {
    const client = new Client()
    await client.connect()
    return new PostgreSqlKeyValueStore(client)
  }
  async get(key: Bytes): Promise<Bytes | null> {
    const res = await this.client.query('SELECT * FROM kvs WHERE key = $1', [
      key.toHexString()
    ])
    return Bytes.fromHexString(res.rows[0].value)
  }
  async put(key: Bytes, value: Bytes): Promise<void> {
    await this.client.query('INSERT INTO kvs(key, value) VALUES($1, $2)', [
      key.toHexString(),
      value.toHexString()
    ])
  }
  del(key: Bytes): Promise<void> {
    throw new Error('Method not implemented.')
  }
  batch(operations: BatchOperation[]): Promise<void> {
    throw new Error('Method not implemented.')
  }
  iter(prefix: Bytes): Promise<Iterator> {
    throw new Error('Method not implemented.')
  }
  bucket(key: Bytes): wakkanay.db.KeyValueStore {
    throw new Error('Method not implemented.')
  }
}
