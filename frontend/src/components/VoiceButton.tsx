import { Mic, MicOff } from 'lucide-react';
import { useVoice } from '../context/VoiceContext';
import { useAuth } from '../context/AuthContext';

export function VoiceButton() {
  const { user } = useAuth();
  const { isListening, isSupported, toggleListening, error } = useVoice();

  if (!user?.voice_enabled) return null;

  if (!isSupported) {
    return (
      <div className="relative group">
        <button
          disabled
          className="p-1.5 rounded-lg text-gray-400 cursor-not-allowed"
          title="Voice control not supported in this browser"
        >
          <MicOff className="h-4 w-4" />
        </button>
        <div className="absolute right-0 top-full mt-1 bg-gray-800 text-white text-xs px-2 py-1 rounded whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity z-50">
          Voice not supported — use Chrome or Edge
        </div>
      </div>
    );
  }

  return (
    <button
      onClick={toggleListening}
      className={`relative p-1.5 rounded-lg transition-all ${
        isListening
          ? 'bg-red-500 text-white shadow-lg shadow-red-500/30'
          : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'
      }`}
      title={isListening ? 'Stop listening (continuous mode)' : 'Voice control — click to start'}
    >
      {isListening ? (
        <>
          <Mic className="h-4 w-4" />
          <span className="absolute inset-0 rounded-lg bg-red-400 animate-ping opacity-30" />
        </>
      ) : (
        <Mic className="h-4 w-4" />
      )}
      {error && (
        <span className="absolute -bottom-1 -right-1 w-2 h-2 bg-amber-400 rounded-full" />
      )}
    </button>
  );
}
