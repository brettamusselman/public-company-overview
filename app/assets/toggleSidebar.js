window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        toggleSidebar: function(openClicks, closeClicks, sidebarStyle, openStyle, closeStyle) {
            if (openClicks && (!closeClicks || openClicks > closeClicks)) {
                return [{...sidebarStyle, display: 'block'}, {...openStyle, display: 'none'}, {...closeStyle, display: 'inline-block'}];
            } else {
                return [{...sidebarStyle, display: 'none'}, {...openStyle, display: 'inline-block'}, {...closeStyle, display: 'none'}];
            }
        }
    }
});

