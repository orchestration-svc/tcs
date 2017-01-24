package net.tcs.core;

public final class TCSCommand {
    public TCSCommand(TCSCommandType commandType, Object body) {
        this.commandType = commandType;
        this.body = body;
    }

    public Object getBody() {
        return body;
    }

    final TCSCommandType commandType;
    final Object body;
}
