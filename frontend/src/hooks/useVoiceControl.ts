import { useState, useRef, useCallback, useEffect } from 'react';

interface UseVoiceControlReturn {
  isListening: boolean;
  transcript: string;
  alternatives: string[];
  interimTranscript: string;
  isSupported: boolean;
  isContinuous: boolean;
  startListening: () => void;
  stopListening: () => void;
  setContinuous: (on: boolean) => void;
  error: string | null;
}

interface SpeechRecognitionEvent {
  results: SpeechRecognitionResultList;
  resultIndex: number;
}

// Audio feedback — short confirmation tone
const CONFIRM_TONE_FREQ = 880;
const CONFIRM_TONE_DURATION = 80;
let audioCtx: AudioContext | null = null;

export function playConfirmTone() {
  try {
    if (!audioCtx) audioCtx = new AudioContext();
    const osc = audioCtx.createOscillator();
    const gain = audioCtx.createGain();
    osc.connect(gain);
    gain.connect(audioCtx.destination);
    osc.frequency.value = CONFIRM_TONE_FREQ;
    gain.gain.value = 0.15;
    osc.start();
    gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + CONFIRM_TONE_DURATION / 1000);
    osc.stop(audioCtx.currentTime + CONFIRM_TONE_DURATION / 1000);
  } catch { /* audio not available */ }
}

export function useVoiceControl(): UseVoiceControlReturn {
  const [isListening, setIsListening] = useState(false);
  const [transcript, setTranscript] = useState('');
  const [alternatives, setAlternatives] = useState<string[]>([]);
  const [interimTranscript, setInterimTranscript] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isContinuous, setIsContinuous] = useState(true);
  const recognitionRef = useRef<any>(null);
  const continuousRef = useRef(true);
  const wantListeningRef = useRef(false);

  const isSupported = typeof window !== 'undefined' && (
    'SpeechRecognition' in window || 'webkitSpeechRecognition' in window
  );

  // Keep ref in sync
  useEffect(() => { continuousRef.current = isContinuous; }, [isContinuous]);

  // Initialize recognition
  useEffect(() => {
    if (!isSupported) return;

    const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
    const recognition = new SpeechRecognition();

    recognition.continuous = false; // We handle restart ourselves for reliability
    recognition.interimResults = true;
    recognition.lang = 'en-GB';
    recognition.maxAlternatives = 5; // Get multiple guesses for better matching

    recognition.onresult = (event: SpeechRecognitionEvent) => {
      let interim = '';
      let final = '';
      const alts: string[] = [];

      for (let i = event.resultIndex; i < event.results.length; i++) {
        const result = event.results[i];
        if (result.isFinal) {
          final += result[0].transcript;
          // Collect all alternatives
          for (let j = 0; j < result.length; j++) {
            alts.push(result[j].transcript.trim().toLowerCase());
          }
        } else {
          interim += result[0].transcript;
        }
      }

      if (final) {
        setTranscript(final.trim().toLowerCase());
        setAlternatives(alts);
        setInterimTranscript('');
      } else {
        setInterimTranscript(interim.trim());
      }
    };

    recognition.onerror = (event: any) => {
      if (event.error === 'no-speech' || event.error === 'aborted') {
        setError(null);
      } else {
        setError(event.error || 'Speech recognition error');
      }
      setIsListening(false);
    };

    recognition.onend = () => {
      setIsListening(false);
      // Auto-restart in continuous mode
      if (wantListeningRef.current && continuousRef.current) {
        setTimeout(() => {
          if (wantListeningRef.current) {
            try {
              recognition.start();
              setIsListening(true);
            } catch { /* already started or disposed */ }
          }
        }, 200);
      }
    };

    recognitionRef.current = recognition;

    return () => {
      wantListeningRef.current = false;
      try { recognition.abort(); } catch { /* ignore */ }
    };
  }, [isSupported]);

  const startListening = useCallback(async () => {
    if (!recognitionRef.current) return;

    // Explicitly request mic permission first — some browsers won't prompt
    // via SpeechRecognition.start() alone
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      // Release the stream immediately — we just needed the permission grant
      stream.getTracks().forEach(t => t.stop());
    } catch (err: any) {
      setError(err.name === 'NotAllowedError'
        ? 'Microphone access denied — check browser permissions'
        : `Microphone error: ${err.message}`);
      return;
    }

    wantListeningRef.current = true;
    setTranscript('');
    setAlternatives([]);
    setInterimTranscript('');
    setError(null);

    if (isListening) return; // Already running

    try {
      recognitionRef.current.start();
      setIsListening(true);
    } catch {
      setError('Could not start voice recognition');
    }
  }, [isListening]);

  const stopListening = useCallback(() => {
    wantListeningRef.current = false;
    if (!recognitionRef.current) return;

    try {
      recognitionRef.current.stop();
    } catch { /* ignore */ }
    setIsListening(false);
  }, []);

  const setContinuous = useCallback((on: boolean) => {
    setIsContinuous(on);
  }, []);

  return {
    isListening,
    transcript,
    alternatives,
    interimTranscript,
    isSupported,
    isContinuous,
    startListening,
    stopListening,
    setContinuous,
    error,
  };
}
