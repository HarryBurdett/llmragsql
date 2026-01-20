import { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Send, Brain, MessageSquare, Database, Loader2 } from 'lucide-react';
import apiClient from '../api/client';
import type { RAGQueryResponse } from '../api/client';

interface Message {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  sources?: { score: number; text: string }[];
  timestamp: Date;
}

export function Ask() {
  const [question, setQuestion] = useState('');
  const [messages, setMessages] = useState<Message[]>([]);
  const [numResults, setNumResults] = useState(5);

  // Get vector stats
  const { data: vectorStats } = useQuery({
    queryKey: ['vectorStats'],
    queryFn: () => apiClient.getVectorStats(),
  });

  // RAG query mutation
  const queryMutation = useMutation({
    mutationFn: (q: string) => apiClient.ragQuery(q, numResults),
    onSuccess: (response) => {
      const data = response.data as RAGQueryResponse;
      const assistantMessage: Message = {
        id: Date.now().toString(),
        type: 'assistant',
        content: data.success ? data.answer : data.error || 'An error occurred',
        sources: data.sources,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
    },
    onError: () => {
      const errorMessage: Message = {
        id: Date.now().toString(),
        type: 'assistant',
        content: 'Failed to get a response. Please check your LLM configuration.',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!question.trim() || queryMutation.isPending) return;

    // Add user message
    const userMessage: Message = {
      id: Date.now().toString(),
      type: 'user',
      content: question,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMessage]);

    // Clear input and send query
    const q = question;
    setQuestion('');
    queryMutation.mutate(q);
  };

  const handleClearChat = () => {
    setMessages([]);
  };

  const vectorCount = vectorStats?.data?.stats?.vectors_count || 0;

  return (
    <div className="space-y-6 h-[calc(100vh-12rem)]">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Ask Questions</h2>
          <p className="text-gray-600 mt-1">Query your data using natural language</p>
        </div>
        <div className="flex items-center space-x-4">
          <div className="text-sm text-gray-500">
            <Database className="h-4 w-4 inline mr-1" />
            {vectorCount} vectors indexed
          </div>
          <button onClick={handleClearChat} className="btn btn-secondary text-sm">
            Clear Chat
          </button>
        </div>
      </div>

      <div className="card flex flex-col h-full">
        {/* Settings Bar */}
        <div className="flex items-center justify-between pb-4 border-b border-gray-200">
          <div className="flex items-center space-x-4">
            <label className="text-sm text-gray-600">
              Results to retrieve:
              <select
                value={numResults}
                onChange={(e) => setNumResults(parseInt(e.target.value))}
                className="ml-2 px-2 py-1 border border-gray-300 rounded text-sm"
              >
                <option value={3}>3</option>
                <option value={5}>5</option>
                <option value={10}>10</option>
                <option value={20}>20</option>
              </select>
            </label>
          </div>
        </div>

        {/* Messages Area */}
        <div className="flex-1 overflow-y-auto py-4 space-y-4 min-h-[400px]">
          {messages.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              <Brain className="h-12 w-12 mx-auto mb-4 text-gray-400" />
              <p className="text-lg font-medium">Ask a question about your data</p>
              <p className="text-sm mt-2">
                First, ingest data using the Database page, then ask questions here.
              </p>
            </div>
          ) : (
            messages.map((message) => (
              <div
                key={message.id}
                className={`flex ${message.type === 'user' ? 'justify-end' : 'justify-start'}`}
              >
                <div
                  className={`max-w-3xl rounded-lg px-4 py-3 ${
                    message.type === 'user'
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 text-gray-800'
                  }`}
                >
                  <div className="flex items-start space-x-2">
                    {message.type === 'assistant' && (
                      <Brain className="h-5 w-5 text-blue-600 mt-0.5 flex-shrink-0" />
                    )}
                    {message.type === 'user' && (
                      <MessageSquare className="h-5 w-5 text-white mt-0.5 flex-shrink-0" />
                    )}
                    <div className="flex-1">
                      <p className="whitespace-pre-wrap">{message.content}</p>
                      {message.sources && message.sources.length > 0 && (
                        <div className="mt-3 pt-3 border-t border-gray-200">
                          <p className="text-xs font-semibold text-gray-500 mb-2">Sources:</p>
                          <div className="space-y-2">
                            {message.sources.map((source, idx) => (
                              <div
                                key={idx}
                                className="text-xs bg-white p-2 rounded border border-gray-200"
                              >
                                <span className="text-blue-600 font-medium">
                                  Score: {(source.score * 100).toFixed(1)}%
                                </span>
                                <p className="text-gray-600 mt-1">{source.text}</p>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ))
          )}
          {queryMutation.isPending && (
            <div className="flex justify-start">
              <div className="bg-gray-100 rounded-lg px-4 py-3">
                <Loader2 className="h-5 w-5 animate-spin text-blue-600" />
              </div>
            </div>
          )}
        </div>

        {/* Input Area */}
        <div className="pt-4 border-t border-gray-200">
          <form onSubmit={handleSubmit} className="flex space-x-3">
            <input
              type="text"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Ask a question about your data..."
              className="input flex-1"
              disabled={queryMutation.isPending}
            />
            <button
              type="submit"
              disabled={queryMutation.isPending || !question.trim()}
              className="btn btn-primary flex items-center"
            >
              <Send className="h-4 w-4 mr-2" />
              Send
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
