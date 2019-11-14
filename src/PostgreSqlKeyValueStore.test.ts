import { Client } from 'pg'
import { PostgreSqlKeyValueStore } from './PostgreSqlKeyValueStore'
import { Bytes } from 'wakkanay/dist/types/Codables'

const testDbName = Bytes.fromString('test_pg')
const testKey = Bytes.fromString('test_key')
const testValue = Bytes.fromString('test_value')

jest.mock('pg', () => {
  return {
    Client: function() {
      return {
        connect: () => {},
        query: async () => {
          return {
            rows: [
              { key: testKey.toHexString(), value: testValue.toHexString() }
            ]
          }
        }
      }
    }
  }
})

describe('PostgreSqlKeyValueStore', () => {
  describe('put', () => {
    it('suceed to put', async () => {
      const kvs = await PostgreSqlKeyValueStore.open(testDbName)
      await kvs.put(testKey, testValue)
    })
  })
  describe('get', () => {
    it('suceed to get', async () => {
      const kvs = await PostgreSqlKeyValueStore.open(testDbName)
      const value = await kvs.get(testKey)
      expect(value).toEqual(testValue)
    })
  })
})
