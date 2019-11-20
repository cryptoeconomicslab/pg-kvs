import { Client } from 'pg'
import { ByteUtils, PostgreSqlKeyValueStore } from './PostgreSqlKeyValueStore'
import { PostgreSqlRangeDb } from './PostgreSqlRangeDb'
import { Bytes } from 'wakkanay/dist/types/Codables'

const testDbName = Bytes.fromString('test_pg')
const testKey = Bytes.fromString('test_key')
const testValue = Bytes.fromString('test_value')

const mockQuery = jest
  .fn()
  .mockImplementation(async (queryText, queryParams) => {
    if (!queryParams) {
      return
    }
    const start = queryParams[0]
    const end = queryParams[1]
    if (start == 1000) {
      throw new Error('connection refused')
    }
    return {
      rows: [
        {
          range_start: 100,
          range_end: 200,
          value: ByteUtils.bytesToBuffer(testValue)
        },
        {
          range_start: 200,
          range_end: 300,
          value: ByteUtils.bytesToBuffer(testValue)
        }
      ].filter(r => r.range_start <= end && r.range_end > start)
    }
  })

jest.mock('pg', () => {
  return {
    Client: jest.fn().mockImplementation(() => {
      return {
        connect: () => {},
        end: () => {},
        query: mockQuery
      }
    })
  }
})

describe('PostgreSqlRangeDb', () => {
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
      const rangeDb = new PostgreSqlRangeDb(kvs)
      await rangeDb.put(10, 20, testValue)
      expect(mockQuery).toHaveBeenCalledTimes(6)
    })
    it('suceed to put a range and update existing', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      await rangeDb.put(50, 150, testValue)
      expect(mockQuery).toHaveBeenCalledTimes(7)
    })
    it('suceed to update a range within existing range', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      await rangeDb.put(110, 120, testValue)
      expect(mockQuery).toHaveBeenCalledTimes(8)
      expect(mockQuery.mock.calls[7][0]).toBe('COMMIT')
    })
    it('rollback', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      await expect(rangeDb.put(1000, 1050, testValue)).rejects.toEqual(
        new Error('connection refused')
      )
      expect(mockQuery.mock.calls[4][0]).toBe('ROLLBACK')
    })
  })
  describe('get', () => {
    it('suceed to get', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      const ranges = await rangeDb.get(100, 110)
      expect(ranges.length).toBe(1)
      expect(ranges[0].start).toBe(100)
      expect(ranges[0].end).toBe(200)
      expect(ranges[0].value).toEqual(testValue)
      expect(mockQuery).toHaveBeenCalledTimes(3)
    })
    it('get nothing', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      const ranges = await rangeDb.get(500, 600)
      expect(ranges).toEqual([])
    })
    it('get multiple ranges', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      const ranges = await rangeDb.get(100, 250)
      expect(ranges.length).toBe(2)
    })
  })
  describe('del', () => {
    it('suceed to del', async () => {
      const rangeDb = new PostgreSqlRangeDb(kvs)
      await rangeDb.del(0, 50)
      expect(mockQuery).toHaveBeenCalledTimes(3)
    })
  })
})
