window.addEventListener("load", (ev) => {
    let log = document.querySelector("#log");
    log.textContent += "loaded";
    fetch("stats")
        .then(x => {
            log.textContent += " Stats A ret";
            x.json().then(js => {
                log.textContent += js;
            });
        })
    fetch("../stats")
        .then(x => {
            log.textContent += " Stats B ret";
            x.json().then(js => {
                log.textContent += js;
            });
        })
})
