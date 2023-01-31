// Using playwright with chromium, open each html file in ./maps and take a screenshot and save it to ./screenshots
// Usage: node screenshot_maps.js
// install playwright with chromium: npm install playwright --save-dev

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');


const mapsDir = path.join(__dirname, 'maps');
const screenshotsDir = path.join(__dirname, 'screenshots');

const files = fs.readdirSync(mapsDir);

(async () => {
	const browser = await chromium.launch();
	const context = await browser.newContext();
	const page = await context.newPage();

	for (const file of files) {
		const filePath = path.join(mapsDir, file);
		await page.goto(`file://${filePath}`);
		await page.screenshot({ path: path.join(screenshotsDir, file + '.png') });
	}

	await browser.close();
})();