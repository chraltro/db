import { useState, useRef, useCallback } from "react";

export default function useResizable(storageKey, defaultSize, minSize, maxSize) {
  const [size, setSize] = useState(() => {
    const stored = localStorage.getItem(storageKey);
    if (stored) {
      const parsed = parseInt(stored, 10);
      if (!isNaN(parsed)) return Math.max(minSize, Math.min(maxSize, parsed));
    }
    return defaultSize;
  });

  const startSizeRef = useRef(size);

  const onResizeStart = useCallback(() => {
    startSizeRef.current = size;
  }, [size]);

  const onResize = useCallback((delta) => {
    const newSize = Math.max(minSize, Math.min(maxSize, startSizeRef.current + delta));
    setSize(newSize);
    localStorage.setItem(storageKey, String(Math.round(newSize)));
  }, [storageKey, minSize, maxSize]);

  return [size, onResize, onResizeStart];
}
