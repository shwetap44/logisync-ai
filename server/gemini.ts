/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { GoogleGenAI } from '@google/genai';

let aiClient: GoogleGenAI | null = null;

/**
 * LAZY SDK INITIALIZATION
 * 
 * Analogy: Think of the GoogleGenAI client as a professional heavy machine.
 * Instead of starting the machine the split second our building turns on (which would crash if power is unstable or keys are missing),
 * we only boot the machine up the first time a worker actually needs to use it!
 */
export function getAiClient(): GoogleGenAI | null {
  if (!aiClient) {
    const key = process.env.GEMINI_API_KEY?.trim();
    const placeholders = new Set([
      '',
      'MY_GEMINI_API_KEY',
      'PLACEHOLDER_API_KEY',
      'your_gemini_api_key_here',
      'your_real_key_here',
    ]);

    if (key && !placeholders.has(key)) {
      try {
        aiClient = new GoogleGenAI({ apiKey: key });
      } catch (err) {
        console.error('Failed to initialize GoogleGenAI SDK:', err);
      }
    }
  }
  return aiClient;
}
