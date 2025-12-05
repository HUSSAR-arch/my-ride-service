require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

console.log('--- CONFIG CHECK ---');
console.log('URL:', url);
console.log('Key Length:', key ? key.length : 'MISSING');

if (!url || !url.startsWith('https://')) {
  console.error(
    "!!! ERROR: SUPABASE_URL is missing or does not start with 'https://'",
  );
  process.exit(1);
}

const supabase = createClient(url, key);

async function testConnection() {
  console.log('\n--- ATTEMPTING CONNECTION ---');
  try {
    // Try to fetch just 1 row from 'rides' to test the link
    const { data, error } = await supabase.from('rides').select('*').limit(1);

    if (error) {
      console.error('Supabase API returned an error:');
      console.error(error);
    } else {
      console.log('✅ SUCCESS! Connected to Supabase.');
      console.log('Data received:', data);
    }
  } catch (err) {
    console.error('❌ NETWORK/CLIENT ERROR:');
    console.error(err);
  }
}

testConnection();
