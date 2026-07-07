require("dotenv").config();

console.log("API_ID:", process.env.API_ID);
console.log("API_HASH exists:", !!process.env.API_HASH);
console.log("STRING_SESSION exists:", !!process.env.STRING_SESSION);
