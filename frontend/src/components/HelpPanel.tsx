import { HelpCircle, X } from 'lucide-react';

interface HelpSection {
  title: string;
  content: string;
}

interface HelpPanelProps {
  sections: HelpSection[];
  isOpen: boolean;
  onClose: () => void;
}

export function HelpPanel({ sections, isOpen, onClose }: HelpPanelProps) {
  if (!isOpen) return null;

  return (
    <div className="bg-blue-50 border border-blue-200 rounded-xl p-5 text-sm text-blue-900 space-y-4">
      <div className="flex items-start justify-between">
        <h3 className="font-semibold text-base flex items-center gap-2">
          <HelpCircle className="h-5 w-5 text-blue-600" />
          Help
        </h3>
        <button onClick={onClose} className="text-blue-400 hover:text-blue-600">
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="space-y-3">
        {sections.map((section, index) => (
          <div key={index}>
            <h4 className="font-semibold">{section.title}</h4>
            <p className="text-blue-800">{section.content}</p>
          </div>
        ))}
      </div>

      <p className="text-xs text-blue-600 border-t border-blue-200 pt-3">
        Press F1 to toggle this help panel
      </p>
    </div>
  );
}
