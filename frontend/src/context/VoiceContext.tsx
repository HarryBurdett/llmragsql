import { createContext, useContext, useState, useCallback, useRef, useEffect, type ReactNode } from 'react';
import { useNavigate } from 'react-router-dom';
import { useVoiceControl, playConfirmTone } from '../hooks/useVoiceControl';
import { useAuth } from './AuthContext';

export interface VoiceCommand {
  id: string;
  phrases: string[];
  action: () => void;
  description?: string;
}

type VoiceMode = 'navigation' | 'field_search' | 'result_select';

interface VoiceContextType {
  isListening: boolean;
  isSupported: boolean;
  transcript: string;
  interimTranscript: string;
  matchedCommand: string | null;
  voiceMode: VoiceMode;
  isContinuous: boolean;
  toggleListening: () => void;
  setContinuous: (on: boolean) => void;
  registerCommands: (commands: VoiceCommand[]) => () => void;
  setFieldSearchHandler: (handler: ((text: string) => void) | null) => void;
  setResultSelectHandler: (handler: ((text: string) => void) | null) => void;
  setVoiceMode: (mode: VoiceMode) => void;
  error: string | null;
}

const VoiceContext = createContext<VoiceContextType | null>(null);

// ── Direct alias map for navigation ──
// Every key is a phrase that maps to a route + label.
// This is O(1) lookup — no fuzzy matching needed.
const NAV_ALIAS_MAP: Record<string, { route: string; label: string }> = {};

const NAV_ENTRIES: { phrases: string[]; route: string; label: string }[] = [
  { phrases: ['direct debits', 'dd', 'gocardless', 'go cardless', 'direct debit'], route: '/cashbook/gocardless', label: 'Direct Debits' },
  { phrases: ['supplier management', 'suppliers', 'supplier'], route: '/supplier/dashboard', label: 'Supplier Management' },
  { phrases: ['bank reconciliation', 'bank rec', 'bank statements', 'bank statement', 'bank reconcile'], route: '/cashbook/bank-hub', label: 'Bank Reconciliation' },
  { phrases: ['dashboard', 'dash'], route: '/archive/dashboard', label: 'Dashboard' },
  { phrases: ['purchase management', 'purchases', 'purchase'], route: '/supplier/statements/queue', label: 'Purchase Management' },
  { phrases: ['expenses', 'expense'], route: '/expenses', label: 'Expenses' },
  { phrases: ['reconciliation', 'balance check', 'reconcile'], route: '/reconcile/summary', label: 'Reconciliation' },
  { phrases: ['go home', 'home'], route: '/', label: 'Home' },
  { phrases: ['settings', 'setting'], route: '/settings', label: 'Settings' },
  { phrases: ['preferences', 'my preferences'], route: '/my-preferences', label: 'My Preferences' },
];

// Build the flat lookup map
for (const entry of NAV_ENTRIES) {
  for (const phrase of entry.phrases) {
    NAV_ALIAS_MAP[phrase] = { route: entry.route, label: entry.label };
  }
}

// ── Matching engine ──
// Tries all speech alternatives against a command set.
// Returns the first exact match from the alias map, falling back to
// word-overlap scoring if no exact hit.

function matchCommand(
  alternatives: string[],
  commands: VoiceCommand[]
): { command: VoiceCommand; label: string } | null {
  // Build a flat lookup from all registered commands
  const cmdMap = new Map<string, VoiceCommand>();
  for (const cmd of commands) {
    for (const phrase of cmd.phrases) {
      cmdMap.set(phrase.toLowerCase(), cmd);
    }
  }

  // 1. Exact match against any alternative
  for (const alt of alternatives) {
    const hit = cmdMap.get(alt);
    if (hit) return { command: hit, label: hit.description || hit.phrases[0] };
  }

  // 2. "Contains" match — alternative contains a phrase or vice versa
  for (const alt of alternatives) {
    for (const [phrase, cmd] of cmdMap) {
      if (alt.includes(phrase) || phrase.includes(alt)) {
        return { command: cmd, label: cmd.description || cmd.phrases[0] };
      }
    }
  }

  // 3. Word-overlap scoring (fallback)
  let best = { score: 0, cmd: null as VoiceCommand | null };
  for (const alt of alternatives) {
    const words = alt.split(/\s+/);
    for (const cmd of commands) {
      for (const phrase of cmd.phrases) {
        const pWords = phrase.toLowerCase().split(/\s+/);
        const overlap = pWords.filter(pw => words.some(w => w === pw || w.includes(pw) || pw.includes(w)));
        const score = overlap.length / pWords.length;
        if (score > best.score) {
          best = { score, cmd };
        }
      }
    }
  }

  if (best.score >= 0.6 && best.cmd) {
    return { command: best.cmd, label: best.cmd.description || best.cmd.phrases[0] };
  }

  return null;
}

function matchNavigation(
  alternatives: string[]
): { route: string; label: string } | null {
  // 1. Direct alias lookup against all alternatives
  for (const alt of alternatives) {
    const hit = NAV_ALIAS_MAP[alt];
    if (hit) return hit;

    // "open X" pattern
    if (alt.startsWith('open ')) {
      const target = alt.slice(5);
      const hit2 = NAV_ALIAS_MAP[target];
      if (hit2) return hit2;
    }
  }

  // 2. Contains match
  for (const alt of alternatives) {
    for (const [phrase, entry] of Object.entries(NAV_ALIAS_MAP)) {
      if (alt.includes(phrase) || phrase.includes(alt)) {
        return entry;
      }
    }
  }

  return null;
}


export function VoiceProvider({ children }: { children: ReactNode }) {
  const { user } = useAuth();
  const navigate = useNavigate();
  const {
    isListening, transcript, alternatives, interimTranscript,
    isSupported, isContinuous, startListening, stopListening, setContinuous, error
  } = useVoiceControl();

  const [matchedCommand, setMatchedCommand] = useState<string | null>(null);
  const [voiceMode, setVoiceMode] = useState<VoiceMode>('navigation');
  const commandsRef = useRef<VoiceCommand[]>([]);
  const fieldSearchRef = useRef<((text: string) => void) | null>(null);
  const resultSelectRef = useRef<((text: string) => void) | null>(null);

  const voiceEnabled = user?.voice_enabled ?? false;

  const registerCommands = useCallback((commands: VoiceCommand[]) => {
    commandsRef.current = [...commandsRef.current, ...commands];
    const ids = commands.map(c => c.id);
    return () => {
      commandsRef.current = commandsRef.current.filter(c => !ids.includes(c.id));
    };
  }, []);

  const setFieldSearchHandler = useCallback((handler: ((text: string) => void) | null) => {
    fieldSearchRef.current = handler;
  }, []);

  const setResultSelectHandler = useCallback((handler: ((text: string) => void) | null) => {
    resultSelectRef.current = handler;
  }, []);

  const toggleListening = useCallback(() => {
    if (!voiceEnabled || !isSupported) return;
    if (isListening) {
      stopListening();
    } else {
      startListening();
    }
  }, [voiceEnabled, isSupported, isListening, startListening, stopListening]);

  // Show matched feedback with tone
  const showMatch = useCallback((label: string) => {
    playConfirmTone();
    setMatchedCommand(label);
    setTimeout(() => setMatchedCommand(null), 2000);
  }, []);

  // Process transcript when it changes
  useEffect(() => {
    if (!transcript || !voiceEnabled) return;

    const alts = alternatives.length > 0 ? alternatives : [transcript];

    // Don't process voice when user is typing (unless in field/result mode)
    const active = document.activeElement;
    if (active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA' || (active as HTMLElement).contentEditable === 'true')) {
      if (voiceMode !== 'field_search' && voiceMode !== 'result_select') return;
    }

    // Route based on voice mode
    if (voiceMode === 'field_search' && fieldSearchRef.current) {
      fieldSearchRef.current(transcript);
      showMatch(`Search: "${transcript}"`);
      return;
    }

    if (voiceMode === 'result_select' && resultSelectRef.current) {
      resultSelectRef.current(transcript);
      showMatch(`Select: "${transcript}"`);
      return;
    }

    // Navigation mode — check registered page commands first (using all alternatives)
    const cmdMatch = matchCommand(alts, commandsRef.current);
    if (cmdMatch) {
      cmdMatch.command.action();
      showMatch(cmdMatch.label);
      return;
    }

    // Check global navigation commands
    const navMatch = matchNavigation(alts);
    if (navMatch) {
      navigate(navMatch.route);
      showMatch(navMatch.label);
      return;
    }

    // No match — show what was heard
    setMatchedCommand(`"${transcript}" — not recognised`);
    setTimeout(() => setMatchedCommand(null), 3000);

  }, [transcript, alternatives, voiceEnabled, voiceMode, navigate, showMatch]);

  return (
    <VoiceContext.Provider
      value={{
        isListening,
        isSupported,
        transcript,
        interimTranscript,
        matchedCommand,
        voiceMode,
        isContinuous,
        toggleListening,
        setContinuous,
        registerCommands,
        setFieldSearchHandler,
        setResultSelectHandler,
        setVoiceMode,
        error,
      }}
    >
      {children}
    </VoiceContext.Provider>
  );
}

export function useVoice(): VoiceContextType {
  const context = useContext(VoiceContext);
  if (!context) {
    throw new Error('useVoice must be used within a VoiceProvider');
  }
  return context;
}
