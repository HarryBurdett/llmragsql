import { useEffect, useRef, useState } from 'react';
import { Mic } from 'lucide-react';
import { useVoice } from '../context/VoiceContext';
import { useAuth } from '../context/AuthContext';

// Audio level meter — shows real-time mic activity via Web Audio API
function AudioLevelBars({ isActive, onLevelChange }: { isActive: boolean; onLevelChange?: (hasAudio: boolean) => void }) {
  const [levels, setLevels] = useState<number[]>([0, 0, 0, 0, 0, 0, 0]);
  const streamRef = useRef<MediaStream | null>(null);
  const ctxRef = useRef<AudioContext | null>(null);
  const rafRef = useRef<number>(0);

  useEffect(() => {
    if (!isActive) {
      setLevels([0, 0, 0, 0, 0, 0, 0]);
      if (streamRef.current) {
        streamRef.current.getTracks().forEach(t => t.stop());
        streamRef.current = null;
      }
      if (ctxRef.current) {
        ctxRef.current.close().catch(() => {});
        ctxRef.current = null;
      }
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      return;
    }

    let cancelled = false;

    navigator.mediaDevices.getUserMedia({ audio: true }).then(stream => {
      if (cancelled) { stream.getTracks().forEach(t => t.stop()); return; }
      streamRef.current = stream;
      const ctx = new AudioContext();
      ctxRef.current = ctx;
      const source = ctx.createMediaStreamSource(stream);
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 64;
      analyser.smoothingTimeConstant = 0.5;
      source.connect(analyser);

      const data = new Uint8Array(analyser.frequencyBinCount);

      const tick = () => {
        if (cancelled) return;
        analyser.getByteFrequencyData(data);
        // Pick 7 frequency bands
        const bands = [1, 3, 5, 7, 9, 11, 13];
        const newLevels = bands.map(i => Math.min((data[i] || 0) / 200, 1));
        setLevels(newLevels);
        // Report if any meaningful audio
        const maxLevel = Math.max(...newLevels);
        onLevelChange?.(maxLevel > 0.15);
        rafRef.current = requestAnimationFrame(tick);
      };
      tick();
    }).catch(() => {
      onLevelChange?.(false);
    });

    return () => {
      cancelled = true;
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      if (streamRef.current) {
        streamRef.current.getTracks().forEach(t => t.stop());
        streamRef.current = null;
      }
      if (ctxRef.current) {
        ctxRef.current.close().catch(() => {});
        ctxRef.current = null;
      }
    };
  }, [isActive, onLevelChange]);

  return (
    <div className="flex items-end gap-[3px] h-6">
      {levels.map((level, i) => (
        <div
          key={i}
          className="w-1.5 rounded-full transition-all duration-75"
          style={{
            height: `${Math.max(level * 24, 4)}px`,
            backgroundColor: level > 0.4 ? '#22c55e' : level > 0.15 ? '#eab308' : '#6b7280',
          }}
        />
      ))}
    </div>
  );
}

export function VoiceIndicator() {
  const { user } = useAuth();
  const { isListening, interimTranscript, matchedCommand } = useVoice();
  const [hasAudio, setHasAudio] = useState(false);
  const [noAudioTimeout, setNoAudioTimeout] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>();

  // Show "no audio" warning after 4s of silence
  useEffect(() => {
    if (!isListening) {
      setNoAudioTimeout(false);
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      return;
    }

    timeoutRef.current = setTimeout(() => {
      if (!hasAudio) setNoAudioTimeout(true);
    }, 4000);

    return () => { if (timeoutRef.current) clearTimeout(timeoutRef.current); };
  }, [isListening, hasAudio]);

  // Reset warning when audio detected
  useEffect(() => {
    if (hasAudio) setNoAudioTimeout(false);
  }, [hasAudio]);

  if (!user?.voice_enabled) return null;
  if (!isListening && !matchedCommand) return null;

  return (
    <div className="fixed bottom-14 left-1/2 -translate-x-1/2 z-50 pointer-events-none">
      <div className="bg-gray-900/95 backdrop-blur-sm text-white px-5 py-3 rounded-2xl shadow-2xl flex items-center gap-3 min-w-56 justify-center">
        {isListening && !matchedCommand && (
          <>
            <div className="flex items-center gap-2">
              <Mic className={`h-5 w-5 ${hasAudio ? 'text-green-400' : 'text-red-400'}`} />
              <AudioLevelBars isActive={isListening} onLevelChange={setHasAudio} />
            </div>
            <div className="flex flex-col">
              <span className="text-sm font-medium">
                {interimTranscript || (noAudioTimeout ? 'No audio — check mic' : 'Speak now...')}
              </span>
              {noAudioTimeout && (
                <span className="text-xs text-amber-400">Microphone may be muted</span>
              )}
            </div>
          </>
        )}
        {matchedCommand && (
          <span className="text-sm font-medium text-emerald-300">
            {matchedCommand}
          </span>
        )}
      </div>
    </div>
  );
}
