import { Client } from 'pg'
import { ByteUtils, PostgreSqlKeyValueStore } from './PostgreSqlKeyValueStore'
import { Bytes } from '@cryptoeconomicslab/primitives'

const testBucket = Bytes.fromString('test_bucket')
const testKey = Bytes.fromString('test_key')
const testNotFoundKey = Bytes.fromString('test_not_found_key')
const testValue = Bytes.fromString('test_value')

const mockQuery = jest
  .fn()
  .mockImplementation(async (queryText, queryParams: any[]) => {
    if (!queryParams) {
      return
    }
    const key: Buffer = queryParams[1]
    if (
      ByteUtils.bufferToBytes(key).toHexString() ==
      testNotFoundKey.toHexString()
    ) {
      return {
        rows: []
      }
    } else {
      return {
        rows: [
          {
            key: ByteUtils.bytesToBuffer(testKey),
            value: ByteUtils.bytesToBuffer(testValue)
          }
        ]
      }
    }
  })

jest.mock('pg', () => {
  return {
    Client: function() {
      return {
        connect: () => {},
        end: () => {},
        query: mockQuery
      }
    }
  }
})

describe('PostgreSqlKeyValueStore', () => {
  let kvs: PostgreSqlKeyValueStore
  beforeEach(async () => {
    mockQuery.mockClear()
    const client = new Client()
    kvs = new PostgreSqlKeyValueStore(client)
    await kvs.open()
  })
  afterEach(async () => {
    await kvs.close()
  })
  describe('put', () => {
    it('suceed to put', async () => {
      await kvs.put(testKey, testValue)
      expect(mockQuery).toHaveBeenCalledTimes(3)
    })
  })
  describe('get', () => {
    it('suceed to get', async () => {
      const value = await kvs.get(testKey)
      expect(value).toEqual(testValue)
    })
    it('get null', async () => {
      const value = await kvs.get(testNotFoundKey)
      expect(value).toBeNull()
    })
  })
  describe('bucket', () => {
    describe('put', () => {
      it('suceed to put', async () => {
        const bucket = await kvs.bucket(testBucket)
        await bucket.put(testKey, testValue)
        expect(mockQuery).toHaveBeenCalledTimes(3)
      })
    })
    describe('get', () => {
      it('suceed to get', async () => {
        const bucket = await kvs.bucket(testBucket)
        const value = await bucket.get(testKey)
        expect(value).toEqual(testValue)
      })
      it('get null', async () => {
        const bucket = await kvs.bucket(testBucket)
        const value = await bucket.get(testNotFoundKey)
        expect(value).toBeNull()
      })
    })
  })
})
