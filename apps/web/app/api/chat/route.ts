import { NextResponse } from "next/server";

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const messages = Array.isArray(body?.messages) ? body.messages : [];
    const last = messages.length ? messages[messages.length - 1] : null;
    const userText = String(last?.content || "").slice(0, 400);

    const reply =
      userText.length > 0
        ? `I received: "${userText}". Chat is not wired to a model yet, but I can be connected once you provide an API key.`
        : "Chat is not wired to a model yet. Provide an API key to enable.";

    return NextResponse.json({ message: reply });
  } catch (e: any) {
    return NextResponse.json({ message: "Chat error." }, { status: 500 });
  }
}
