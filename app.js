const axios = require('axios');
const dotenv = require('dotenv');
const _ = require('lodash');
const { Client } = require('@notionhq/client');

dotenv.config();

const NOTION_API_KEY = process.env.NOTION_API_KEY;
const BINANCE_API = 'https://www.binance.com/api/v3/ticker/price';
const BYBIT_API = 'https://api.bybit.com/v5/market/tickers?category=spot';
const OKX_API = 'https://www.okx.com/api/v5/market/tickers?instType=SPOT';
const COINEX_API = 'https://api.coinex.com/v1/market/ticker/all';
const DATABASE_IDS = ("" || process.env.NOTION_DATABASE_IDS).split(',');
const notion = new Client({ auth: process.env.NOTION_API_KEY });
const OPERATION_BATCH_SIZE = 10;

if(!NOTION_API_KEY) {
  console.log(`NOTION_API_KEY should be non empty.`);
}

if(!DATABASE_IDS) {
  console.log(`DATABASE_IDS should be non empty.`);
}

async function getEntriesFromNotionDatabase(database_id) {
  const pages = []
  let cursor = undefined
  while (true) {
    const { results, next_cursor } = await notion.databases.query({
      database_id,
      start_cursor: cursor,
    })
    pages.push(...results)
    if (!next_cursor) {
      break
    }
    cursor = next_cursor
  }
  console.log(`${pages.length} issues successfully fetched.`);
  return pages;
}

async function getTickers(url) {
  try {
    const {data} = await axios.get(url);
    return data;
  } catch (e) {
    console.log(`error while fetching tickers prices ${e?.message}`);
    process.exit(1);
  } 
}

function getPropertiesToUpdate(updates, entries) {
  return entries.map(([key, value]) => {
    const c = {
      pageId: value[0],
      properties: {
        CurrentPrice: {
          number: parseFloat(updates[(value[1] || 'binance')].get(key))
        }
      }
    };
    
    return c;
  });
}

async function updatePages(pageUpdates) {
  const pagesToUpdateChunks = _.chunk(pageUpdates, OPERATION_BATCH_SIZE)
  for (const pagesToUpdateBatch of pagesToUpdateChunks) {
    await Promise.all(
      pagesToUpdateBatch.map(({ pageId, properties }) =>{
        return notion.pages.update({
          page_id: pageId,
          properties
        })}
      )
    )
    console.log(`Completed batch size: ${pagesToUpdateBatch.length}`)
  }
}

function extractBinancePrices(e) {
  return new Map(
    e.filter(({symbol}) => symbol.endsWith('USDT'))
      .map(({symbol, price}) => [symbol.replace('USDT', ''), price])
  );
}

function extractBybitPrices(e) {
  return new Map(
    e?.result?.list?.filter(({symbol}) => symbol.endsWith('USDT'))
      .map(({symbol, bid1Price}) => [symbol.replace('USDT', ''), bid1Price])
  );
}

function extractOkxPrices(e) {
  return new Map(
    e?.data?.filter(({instId}) => instId.endsWith('USDT'))
    .map(({instId, last}) => [instId.replace('-USDT', ''), last])
  );
}

function extractCoinexPrices(e) {
  const tickers = e?.data?.ticker;
  if(!tickers) {
    console.log('error fetching coinex tickers, no tickers');
    return 
  }
  const tickersArray = Object.entries(tickers);
  
  return new Map(
    tickersArray.filter(([key]) => key.endsWith('USDT'))
    .map(([key, values]) => [key.replace('USDT', ''), values.last])
  );
}

async function main() {
  const binanceResp = await getTickers(BINANCE_API);
  const binanceTickers = extractBinancePrices(binanceResp);
  if(!binanceTickers.size) {
    console.log(`No data from binance ${binanceTickers}`);
    process.exit(1);
  }
  
  const bybitResp = await getTickers(BYBIT_API);
  const bybitTickers = extractBybitPrices(bybitResp);
  if(!bybitTickers.size) {
    console.log(`No data from bybit ${bybitTickers}`);
    process.exit(1);
  }

  const okxResp = await getTickers(OKX_API);
  const okxTickers = extractOkxPrices(okxResp);
  if(!okxTickers.size) {
    console.log(`No data from okx ${okxTickers}`);
    process.exit(1);
  }
  
  const coinexResp = await getTickers(COINEX_API);
  const coinexTickers = extractCoinexPrices(coinexResp);
  if(!coinexTickers.size) {
    console.log(`No data from coinex ${coinexTickers}`);
    process.exit(1);
  }
  
  const tickers = {
    bybit: bybitTickers,
    coinex: coinexTickers,
    okx: okxTickers,
    binance: binanceTickers
  };
  
  for(const databaseId of DATABASE_IDS) {
    const entries = await getEntriesFromNotionDatabase(databaseId);
    const entriesMap = entries.map(el => [el?.properties?.Symbol?.title?.[0]?.plain_text, [el?.id, el?.properties?.Exchange?.select?.name]]);
    const pageUpdates = getPropertiesToUpdate(tickers, entriesMap);
    console.log(JSON.stringify(pageUpdates));
    await updatePages(pageUpdates);
  }
}



main();
